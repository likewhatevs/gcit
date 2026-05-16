// Pure config-diff classifier. Produces a `Vec<ReloadAction>` from
// the (old config, new config, currently-running handles) triple.
// The orchestrator in `apply` consumes the actions and threads each
// through registry / state_tx / cancel side effects.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::FlowConfig;

use super::diff::flow_config_unchanged;

/// Per-flow action the supervisor must perform during a config
/// reload. Variants are ordered by lifecycle phase so a reader of
/// the action vector can predict execution order without re-reading
/// `run_reload`'s body.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReloadAction {
    /// Flow's shape is identical to the previous config AND its
    /// handle is currently live. Leave the existing pair alive — no
    /// cancel, no respawn, no FlowRemoved.
    Keep { name: String },
    /// Flow has been removed from the new config. Cancel the live
    /// handle (when present) AND emit `FlowRemoved` so the writer
    /// drops persisted state. `live_handle: false` is the sweep path
    /// for entries that already exited cleanly.
    Remove { name: String, live_handle: bool },
    /// Flow exists in the new config but is disabled. Cancel the
    /// live handle. `url_changed: true` emits FlowRemoved so a future
    /// re-enable does not seed a spurious dispatch on the new source.
    Disable { name: String, url_changed: bool },
    /// Flow exists, is enabled, has a live handle, and its
    /// poll/dispatch shape changed. Cancel + respawn. `url_changed`
    /// triggers FlowRemoved so the new generation doesn't inherit
    /// the old source's last_sha.
    Restart { name: String, url_changed: bool },
    /// Flow exists in the new config + is enabled but has no live
    /// handle. Either a genuinely new flow OR a flow whose handle is
    /// missing due to panic-mid-respawn. Spawn a fresh pair.
    Spawn { name: String },
}

/// Classify every flow that needs a reload action.
///
/// Rules:
///   - in new + enabled + unchanged from old + live → `Keep`
///   - in new + disabled + live → `Disable { url_changed }`
///   - in new + enabled + changed + live → `Restart { url_changed }`
///   - not in new → `Remove { live_handle }`
///   - in new + enabled + not live → `Spawn`
///   - in new + disabled + not live → no action (nothing to cancel;
///     persisted state survives unchanged for future re-enable)
pub(crate) fn compute_reload_actions(
    old_flows: &BTreeMap<String, FlowConfig>,
    new_flows: &BTreeMap<String, FlowConfig>,
    live_handles: &BTreeSet<String>,
) -> Vec<ReloadAction> {
    let mut actions: Vec<ReloadAction> = Vec::new();

    // Union of every name across the three inputs. BTreeSet keeps
    // output order deterministic for tests.
    let mut names: BTreeSet<&str> = BTreeSet::new();
    names.extend(old_flows.keys().map(String::as_str));
    names.extend(new_flows.keys().map(String::as_str));
    names.extend(live_handles.iter().map(String::as_str));

    for name in names {
        let old = old_flows.get(name);
        let new = new_flows.get(name);
        let live = live_handles.contains(name);
        match (new, old, live) {
            (None, Some(_), live_handle) => actions.push(ReloadAction::Remove {
                name: name.to_string(),
                live_handle,
            }),
            // Orphan handle (impossible per registry invariants) —
            // still produce Remove so the consumer cancels it.
            (None, None, true) => actions.push(ReloadAction::Remove {
                name: name.to_string(),
                live_handle: true,
            }),
            (Some(nf), Some(of), true) if !nf.enabled => {
                actions.push(ReloadAction::Disable {
                    name: name.to_string(),
                    url_changed: of.source.url != nf.source.url,
                });
            }
            (Some(nf), None, true) if !nf.enabled => {
                // Disabled with live handle but no old config — unreachable;
                // treat as Disable with no URL change so we at least cancel.
                actions.push(ReloadAction::Disable {
                    name: name.to_string(),
                    url_changed: false,
                });
            }
            (Some(nf), _, false) if !nf.enabled => {
                // Disabled + no live handle + no work; persisted state
                // is preserved for future re-enable.
                continue;
            }
            (Some(nf), Some(of), true) if flow_config_unchanged(of, nf) => {
                actions.push(ReloadAction::Keep {
                    name: name.to_string(),
                });
            }
            (Some(nf), Some(of), true) => {
                actions.push(ReloadAction::Restart {
                    name: name.to_string(),
                    url_changed: of.source.url != nf.source.url,
                });
            }
            // Live handle without an old config entry (impossible per
            // registry invariants) — treat as Restart with
            // url_changed=false.
            (Some(_), None, true) => {
                actions.push(ReloadAction::Restart {
                    name: name.to_string(),
                    url_changed: false,
                });
            }
            (Some(_), _, false) => {
                actions.push(ReloadAction::Spawn {
                    name: name.to_string(),
                });
            }
            (None, None, false) => unreachable!(
                "compute_reload_actions: name in iteration set but absent from all three inputs",
            ),
        }
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActionConfig, CredentialId, Destination, PollOverride, SourceConfig};

    fn cred(id: &str) -> CredentialId {
        CredentialId::new(id).expect("valid credential id")
    }

    fn flow(name: &str, url: &str, enabled: bool, destinations: Vec<Destination>) -> FlowConfig {
        FlowConfig {
            name: name.to_string(),
            enabled,
            description: None,
            source: SourceConfig {
                url: url.to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: None,
            },
            action: ActionConfig::GithubWorkflowDispatch {
                repo: "owner/repo".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: cred("gh"),
                inputs: std::collections::BTreeMap::new(),
            },
            destination: destinations,
            poll: PollOverride::default(),
        }
    }

    fn run(old: Vec<FlowConfig>, new: Vec<FlowConfig>, live: &[&str]) -> Vec<ReloadAction> {
        let old_map: BTreeMap<String, FlowConfig> =
            old.into_iter().map(|f| (f.name.clone(), f)).collect();
        let new_map: BTreeMap<String, FlowConfig> =
            new.into_iter().map(|f| (f.name.clone(), f)).collect();
        let live_set: BTreeSet<String> = live.iter().map(|s| s.to_string()).collect();
        compute_reload_actions(&old_map, &new_map, &live_set)
    }

    #[test]
    fn compute_reload_actions_keep_for_unchanged_enabled_live_flow() {
        let f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![f.clone()], vec![f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Keep {
                name: "ci".to_string()
            }],
        );
    }

    #[test]
    fn compute_reload_actions_remove_when_flow_drops_out_of_new_config() {
        let old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![old_f], vec![], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Remove {
                name: "ci".to_string(),
                live_handle: true,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_remove_without_live_handle_when_flow_drops_out() {
        // Flow was in old config but its handle exited (panic-mid-
        // respawn or clean-exit-already-dropped). Still emit Remove
        // with live_handle=false so writer drops persisted state.
        let old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![old_f], vec![], &[]);
        assert_eq!(
            actions,
            vec![ReloadAction::Remove {
                name: "ci".to_string(),
                live_handle: false,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_disable_when_new_config_disables_flow() {
        let old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let new_f = flow("ci", "https://example.com/repo.git", false, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Disable {
                name: "ci".to_string(),
                url_changed: false,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_disable_with_url_changed_when_disable_and_url_diff() {
        let old_f = flow("ci", "https://example.com/old.git", true, Vec::new());
        let new_f = flow("ci", "https://example.com/new.git", false, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Disable {
                name: "ci".to_string(),
                url_changed: true,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_no_action_for_disabled_flow_with_no_live_handle() {
        // Disabled in old AND in new, no live handle. No cancel,
        // no FlowRemoved (preserve state for future re-enable).
        let old_f = flow("ci", "https://example.com/repo.git", false, Vec::new());
        let new_f = flow("ci", "https://example.com/repo.git", false, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &[]);
        assert!(actions.is_empty(), "got {actions:?}");
    }

    #[test]
    fn compute_reload_actions_restart_for_changed_enabled_live_flow() {
        // Same URL, different action.repo. Restart with
        // url_changed=false because URL is unchanged.
        let mut old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut old_f.action;
        *repo = "owner/old-repo".to_string();
        let mut new_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut new_f.action;
        *repo = "owner/new-repo".to_string();
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Restart {
                name: "ci".to_string(),
                url_changed: false,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_restart_with_url_changed_for_url_diff() {
        let old_f = flow("ci", "https://example.com/old.git", true, Vec::new());
        let new_f = flow("ci", "https://example.com/new.git", true, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Restart {
                name: "ci".to_string(),
                url_changed: true,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_spawn_for_genuinely_new_flow() {
        let new_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![], vec![new_f], &[]);
        assert_eq!(
            actions,
            vec![ReloadAction::Spawn {
                name: "ci".to_string()
            }],
        );
    }

    #[test]
    fn compute_reload_actions_spawn_for_revived_flow_with_no_live_handle() {
        // Flow in old AND new, enabled, but handle missing
        // (panic-mid-respawn). Spawn fresh; persisted state survives
        // so the new poll loop's baseline reflects the last SHA.
        let f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![f.clone()], vec![f], &[]);
        assert_eq!(
            actions,
            vec![ReloadAction::Spawn {
                name: "ci".to_string()
            }],
        );
    }

    #[test]
    fn compute_reload_actions_handles_multiple_flows_independently() {
        // 4 flows: keep, remove, disable, restart. Output order
        // follows alphabetic name sort.
        let kept_old = flow("kept", "https://example.com/k.git", true, Vec::new());
        let kept_new = kept_old.clone();
        let removed = flow("removed", "https://example.com/r.git", true, Vec::new());
        let disabled_old = flow("dis", "https://example.com/d.git", true, Vec::new());
        let disabled_new = flow("dis", "https://example.com/d.git", false, Vec::new());
        let restart_old = flow("rs", "https://example.com/old.git", true, Vec::new());
        let restart_new = flow("rs", "https://example.com/new.git", true, Vec::new());
        let actions = run(
            vec![kept_old, removed, disabled_old, restart_old],
            vec![kept_new, disabled_new, restart_new],
            &["kept", "removed", "dis", "rs"],
        );
        assert_eq!(
            actions,
            vec![
                ReloadAction::Disable {
                    name: "dis".to_string(),
                    url_changed: false,
                },
                ReloadAction::Keep {
                    name: "kept".to_string(),
                },
                ReloadAction::Remove {
                    name: "removed".to_string(),
                    live_handle: true,
                },
                ReloadAction::Restart {
                    name: "rs".to_string(),
                    url_changed: true,
                },
            ],
        );
    }
}
