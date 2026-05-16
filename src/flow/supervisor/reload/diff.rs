// Flow-shape diff helpers: `flow_config_unchanged` (the "should we
// cancel + respawn?" predicate) plus canonical-multiset destination
// compare and kept-credential collection.

use std::collections::BTreeSet;

use crate::config::{ActionConfig, Config, CredentialId, Destination, FlowConfig, SourceConfig};

/// Returns true when two `FlowConfig`s describe identical
/// poll/dispatch/notify shapes so `run_reload` can keep the existing
/// pair alive. Compares every field except `name` (the map key) and
/// `description` (purely operator-facing — a description tweak should
/// not interrupt monitor tracking).
///
/// Source/action/poll are compared structurally via
/// `serde_json::to_value`; the Serialize impls are canonical
/// (BTreeMap inputs, declared field order). Destinations use a
/// multiset compare because notifier dispatch is order-insensitive —
/// reordering `[[flow.destination]]` should not trigger a respawn.
/// Notifier IDs are positional at spawn time; pure reorders are
/// "unchanged" so existing notifiers keep their IDs.
///
/// On serialization failure (unreachable — every nested type derives
/// Serialize), conservatively returns `false`.
pub(super) fn flow_config_unchanged(old: &FlowConfig, new: &FlowConfig) -> bool {
    if old.enabled != new.enabled {
        return false;
    }
    let Ok(old_v) = serde_json::to_value(FlowDiff::from(old)) else {
        return false;
    };
    let Ok(new_v) = serde_json::to_value(FlowDiff::from(new)) else {
        return false;
    };
    if old_v != new_v {
        return false;
    }
    let Some(old_dests) = canonical_destination_multiset(&old.destination) else {
        return false;
    };
    let Some(new_dests) = canonical_destination_multiset(&new.destination) else {
        return false;
    };
    old_dests == new_dests
}

/// Canonical multiset representation of a destination list. Each
/// destination is serialized to JSON, the inner `fire_on` array is
/// sorted (event order is semantically irrelevant), and the resulting
/// strings are sorted. Different element counts of the same content
/// still surface as different (multiset semantics).
pub(super) fn canonical_destination_multiset(destinations: &[Destination]) -> Option<Vec<String>> {
    let mut canonical: Vec<String> = Vec::with_capacity(destinations.len());
    for d in destinations {
        let mut v = serde_json::to_value(d).ok()?;
        canonicalize_fire_on(&mut v);
        canonical.push(serde_json::to_string(&v).ok()?);
    }
    canonical.sort();
    Some(canonical)
}

/// Sort the `fire_on` array (if any) inside a destination's JSON.
fn canonicalize_fire_on(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value {
        if let Some(serde_json::Value::Array(arr)) = map.get_mut("fire_on") {
            arr.sort_by_key(|a| a.to_string());
        }
    }
}

/// CredentialIds referenced by every kept-alive flow's action AND
/// every Discord destination on a kept-alive flow. Used to feed
/// `CredentialPool::invalidate_except` so kept-alive flows keep
/// their shared rate-limit poller, cached secret, and
/// `Arc<GithubClient>` across the reload.
///
/// LocalMail destinations have no credential to track.
pub(super) fn collect_kept_credentials(
    new_cfg: &Config,
    to_keep: &BTreeSet<String>,
) -> BTreeSet<CredentialId> {
    let mut keep: BTreeSet<CredentialId> = BTreeSet::new();
    for flow in &new_cfg.flow {
        if !to_keep.contains(&flow.name) {
            continue;
        }
        match &flow.action {
            ActionConfig::GithubWorkflowDispatch { credential_id, .. } => {
                keep.insert(credential_id.clone());
            }
        }
        for dest in &flow.destination {
            if let Destination::DiscordWebhook(w) = dest {
                keep.insert(w.credential_id.clone());
            }
        }
    }
    keep
}

/// Subset of `FlowConfig` excluding `name` and `description`.
/// Destinations are compared separately as a multiset.
#[derive(serde::Serialize)]
struct FlowDiff<'a> {
    source: &'a SourceConfig,
    action: &'a ActionConfig,
    poll: &'a crate::config::PollOverride,
}

impl<'a> From<&'a FlowConfig> for FlowDiff<'a> {
    fn from(f: &'a FlowConfig) -> Self {
        Self {
            source: &f.source,
            action: &f.action,
            poll: &f.poll,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        DiscordTemplateConfig, DiscordWebhookConfig, FireEvent, LocalMailConfig,
        LocalMailTemplateConfig, PollOverride, SourceConfig,
    };
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::util::test_cred as cred;

    fn discord_dest(id: &str, fire_on: Vec<FireEvent>) -> Destination {
        Destination::DiscordWebhook(DiscordWebhookConfig {
            credential_id: cred(id),
            fire_on,
            template: DiscordTemplateConfig::default(),
        })
    }

    fn mail_dest(user: &str, fire_on: Vec<FireEvent>) -> Destination {
        Destination::LocalMail(LocalMailConfig {
            user: user.to_string(),
            fire_on,
            template: LocalMailTemplateConfig::default(),
        })
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
                inputs: BTreeMap::new(),
            },
            destination: destinations,
            poll: PollOverride::default(),
        }
    }

    fn flow_with_action_cred(
        name: &str,
        action_cred: &str,
        destinations: Vec<Destination>,
    ) -> FlowConfig {
        let mut f = flow(name, "https://example.com/repo.git", true, destinations);
        let ActionConfig::GithubWorkflowDispatch { credential_id, .. } = &mut f.action;
        *credential_id = cred(action_cred);
        f
    }

    fn cfg_with_flows(flows: Vec<FlowConfig>) -> Config {
        Config {
            source_path: std::path::PathBuf::new(),
            poll: crate::config::PollDefaults::default(),
            log: crate::config::LogConfig::default(),
            http: crate::config::HttpConfig::default(),
            flow: flows,
            credential_lines: BTreeMap::new(),
        }
    }

    fn keep_set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn flow_config_unchanged_treats_destination_reorder_as_unchanged() {
        let d_a = discord_dest("hookA", vec![FireEvent::RunComplete]);
        let d_b = mail_dest("alice", vec![FireEvent::RunComplete]);
        let old = flow("f", "u", true, vec![d_a.clone(), d_b.clone()]);
        let new = flow("f", "u", true, vec![d_b, d_a]);
        assert!(flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_treats_fire_on_reorder_as_unchanged() {
        let old = flow(
            "f",
            "u",
            true,
            vec![discord_dest(
                "hookA",
                vec![FireEvent::RunStart, FireEvent::RunComplete],
            )],
        );
        let new = flow(
            "f",
            "u",
            true,
            vec![discord_dest(
                "hookA",
                vec![FireEvent::RunComplete, FireEvent::RunStart],
            )],
        );
        assert!(flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_destination_content_change() {
        let old = flow(
            "f",
            "u",
            true,
            vec![discord_dest("hookA", vec![FireEvent::RunComplete])],
        );
        let new = flow(
            "f",
            "u",
            true,
            vec![discord_dest("hookB", vec![FireEvent::RunComplete])],
        );
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_distinguishes_duplicate_count() {
        let dest = discord_dest("hookA", vec![FireEvent::RunComplete]);
        let old = flow("f", "u", true, vec![dest.clone(), dest.clone()]);
        let new = flow("f", "u", true, vec![dest]);
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_url_change() {
        let old = flow("f", "https://example.com/old.git", true, Vec::new());
        let new = flow("f", "https://example.com/new.git", true, Vec::new());
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_enabled_change_short_circuits() {
        // enabled is checked first — a flip returns false before
        // descending into FlowDiff comparison. Drives the
        // classifier's Disable / Spawn arm rather than Keep.
        let old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let new = flow("f", "https://example.com/repo.git", false, Vec::new());
        assert!(!flow_config_unchanged(&old, &new));
        assert!(!flow_config_unchanged(&new, &old));
    }

    #[test]
    fn flow_config_unchanged_ignores_description_change() {
        // description is omitted from FlowDiff so a description tweak
        // does not interrupt in-flight monitor tracking.
        let mut old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let mut new = flow("f", "https://example.com/repo.git", true, Vec::new());
        old.description = Some("CI pipeline".to_string());
        new.description = Some("CI pipeline (rev 2)".to_string());
        assert!(flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_action_repo_change() {
        // Repo rename (same URL, same workflow) must respawn —
        // otherwise the dispatcher fires into the old repo's
        // workflow_dispatch endpoint.
        let mut old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let mut new = flow("f", "https://example.com/repo.git", true, Vec::new());
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut old.action;
        *repo = "owner/old-repo".to_string();
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut new.action;
        *repo = "owner/new-repo".to_string();
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_poll_override_change() {
        // PollOverride change must invalidate the live pair so the
        // new cadence takes effect.
        let mut old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let mut new = flow("f", "https://example.com/repo.git", true, Vec::new());
        old.poll.source_interval = Some(Duration::from_secs(60));
        new.poll.source_interval = Some(Duration::from_secs(120));
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn canonical_destination_multiset_sorts_outer_and_fire_on() {
        let d_a = discord_dest("a", vec![FireEvent::RunStart, FireEvent::RunComplete]);
        let d_b = mail_dest("u", vec![FireEvent::JobComplete]);
        let canon_one = canonical_destination_multiset(&[d_a.clone(), d_b.clone()]).unwrap();
        let canon_two = canonical_destination_multiset(&[
            mail_dest("u", vec![FireEvent::JobComplete]),
            discord_dest("a", vec![FireEvent::RunComplete, FireEvent::RunStart]),
        ])
        .unwrap();
        assert_eq!(canon_one, canon_two);
    }

    #[test]
    fn collect_kept_credentials_includes_action_credential() {
        let f = flow_with_action_cred("kept", "ghpat", Vec::new());
        let cfg = cfg_with_flows(vec![f]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn collect_kept_credentials_includes_discord_destination_credential() {
        let f = flow_with_action_cred(
            "kept",
            "ghpat",
            vec![discord_dest("hookA", vec![FireEvent::RunComplete])],
        );
        let cfg = cfg_with_flows(vec![f]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert!(keep.contains(&cred("hookA")));
        assert_eq!(keep.len(), 2);
    }

    #[test]
    fn collect_kept_credentials_excludes_local_mail_destinations() {
        let f = flow_with_action_cred(
            "kept",
            "ghpat",
            vec![mail_dest("alice", vec![FireEvent::RunComplete])],
        );
        let cfg = cfg_with_flows(vec![f]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn collect_kept_credentials_skips_non_kept_flows() {
        let kept = flow_with_action_cred("kept", "ghpat", Vec::new());
        let respawning = flow_with_action_cred("respawning", "other", Vec::new());
        let cfg = cfg_with_flows(vec![kept, respawning]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert!(!keep.contains(&cred("other")));
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn collect_kept_credentials_dedups_shared_credentials() {
        let a = flow_with_action_cred("a", "ghpat", Vec::new());
        let b = flow_with_action_cred(
            "b",
            "ghpat",
            vec![discord_dest("ghpat", vec![FireEvent::RunComplete])],
        );
        let cfg = cfg_with_flows(vec![a, b]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["a", "b"]));
        assert_eq!(keep.len(), 1);
        assert!(keep.contains(&cred("ghpat")));
    }
}
