// Conclusion → Discord embed color.
//
// `should_collapse` and `label_for` live in `crate::github` because
// they describe `Conclusion` semantics (not Discord-specific
// rendering); both Discord and mail notifiers consume them.
//
// Tests pin the exact RGB values via tests/discord_color_mapping.rs;
// drift here would change embed strip colors operators rely on for
// at-a-glance status reading.

use crate::github::Conclusion;

/// RGB color for an embed strip, packed as 0xRRGGBB. Discord renders
/// the strip on the left edge of an embed; the color is the operator's
/// first visual cue.
///
/// Palette:
/// - Success → green (#3ba55d, Discord's "online" green).
/// - Skipped → gray (#747f8d, Discord's "offline" gray).
/// - Neutral → slate (visually distinct from both Success and Skipped).
/// - Failure → red (#ed4245, Discord's "danger" red).
/// - TimedOut → amber (#faa61a, Discord's "warning" amber).
/// - Cancelled → gray (treated like Skipped — operator stopped, not a failure).
/// - ActionRequired → purple (#9b59b6, distinct from amber).
/// - Unknown → slate (defensive: future Conclusion additions land here).
pub fn color_for(c: Conclusion) -> u32 {
    match c {
        Conclusion::Success => 0x3b_a5_5d,
        Conclusion::Skipped => 0x74_7f_8d,
        Conclusion::Neutral => 0x99_aa_b5,
        Conclusion::Failure => 0xed_42_45,
        Conclusion::TimedOut => 0xfa_a6_1a,
        Conclusion::Cancelled => 0x74_7f_8d,
        Conclusion::ActionRequired => 0x9b_59_b6,
        Conclusion::Unknown => 0x99_aa_b5,
    }
}

/// Color when the run has not produced a `Conclusion` yet (queued /
/// in-progress / waiting). Discord renders a neutral slate so the
/// operator can tell "still running" from "completed neutral".
pub const COLOR_IN_PROGRESS: u32 = 0x99_aa_b5;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn color_for_pins_palette() {
        // Each variant has a stable hex; pin to catch drift.
        assert_eq!(color_for(Conclusion::Success), 0x3b_a5_5d);
        assert_eq!(color_for(Conclusion::Skipped), 0x74_7f_8d);
        assert_eq!(color_for(Conclusion::Neutral), 0x99_aa_b5);
        assert_eq!(color_for(Conclusion::Failure), 0xed_42_45);
        assert_eq!(color_for(Conclusion::TimedOut), 0xfa_a6_1a);
        assert_eq!(color_for(Conclusion::Cancelled), 0x74_7f_8d);
        assert_eq!(color_for(Conclusion::ActionRequired), 0x9b_59_b6);
        assert_eq!(color_for(Conclusion::Unknown), 0x99_aa_b5);
    }

    #[test]
    fn color_for_within_rgb_range() {
        // twilight-validate rejects color > COLOR_MAXIMUM (0xff_ff_ff).
        // Pin every variant to be within the legal RGB space so embed
        // validation never rejects a gcit-emitted color.
        for c in [
            Conclusion::Success,
            Conclusion::Skipped,
            Conclusion::Neutral,
            Conclusion::Failure,
            Conclusion::TimedOut,
            Conclusion::Cancelled,
            Conclusion::ActionRequired,
            Conclusion::Unknown,
        ] {
            let rgb = color_for(c);
            assert!(rgb <= 0xff_ff_ff, "{c:?} -> 0x{rgb:06x} exceeds RGB max",);
        }
        const { assert!(COLOR_IN_PROGRESS <= 0xff_ff_ff) };
    }
}
