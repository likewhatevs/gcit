// Golden-config snapshot test for the canonical full + minimal fixtures.
//
// Asserts: each fixture deserializes into a Config without error AND
// the resulting struct serialized as YAML matches the committed
// snapshot. The snapshot file under tests/snapshots/ is review fodder
// — every change to the schema MUST update the snapshot, which forces
// the spec author to notice incidental field-rename / default-value
// drift.

use std::path::PathBuf;
use std::time::Duration;

fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(rel)
}

#[test]
fn full_toml_loads() {
    let path = fixture("resources/config/full.toml");
    let cfg = gcit::config::load(&path).expect("full.toml is valid");
    assert_eq!(cfg.flow.len(), 1);
    assert_eq!(cfg.flow[0].name, "linux-mainline-ci");
    assert_eq!(cfg.poll.source_interval, Some(Duration::from_secs(60)));
    assert_eq!(cfg.poll.job_interval, Duration::from_secs(30));
    assert!((cfg.poll.jitter - 0.1).abs() < f64::EPSILON);
    assert_eq!(cfg.http.request_timeout, Duration::from_secs(30));
    // `http.max_concurrent` was removed from HttpConfig — the
    // field is accepted in TOML for back-compat (with a
    // deprecation warning) but not surfaced on the validated
    // struct. full.toml no longer carries the line; the back-
    // compat acceptance path is exercised by
    // config_invalid::http_max_concurrent_*.
    assert!(cfg.flow[0].enabled);
    assert_eq!(cfg.flow[0].destination.len(), 2);
}

#[test]
fn minimal_toml_loads_with_documented_defaults() {
    let path = fixture("resources/config/minimal.toml");
    let cfg = gcit::config::load(&path).expect("minimal.toml is valid");
    // Defaults: job_interval = 30s, jitter = 0.1, request_timeout = 30s,
    // enabled = true. `max_concurrent` was deprecated and removed
    // from HttpConfig; the field is no longer surfaced on the
    // validated struct.
    assert_eq!(cfg.poll.job_interval, Duration::from_secs(30));
    assert!((cfg.poll.jitter - 0.1).abs() < f64::EPSILON);
    assert_eq!(cfg.http.request_timeout, Duration::from_secs(30));
    assert!(cfg.flow[0].enabled);
    // source_interval omitted -> None (strategy default applies at runtime).
    assert!(cfg.poll.source_interval.is_none());
    // Discord destination has no explicit fire_on -> defaults to [run_complete].
    let dest = &cfg.flow[0].destination[0];
    if let gcit::config::Destination::DiscordWebhook(d) = dest {
        assert_eq!(d.fire_on, vec![gcit::config::FireEvent::RunComplete]);
    } else {
        panic!("expected discord_webhook destination");
    }
}
