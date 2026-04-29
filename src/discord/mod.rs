// Discord webhook notifier — submodules + re-exports.
//
// Submodules:
//   - conclusion: Conclusion → color/label/collapse mapping.
//   - template: handlebars rendering + codepoint-safe truncation.
//   - embed: programmatic Embed construction (collapse-aware).
//   - webhook: twilight-http Client wrapper + URL parser.
//   - notifier: DiscordNotifier impl Notifier.
//
// Items here are pub for integration-test reachability and treated as
// crate-internal + unstable.

pub mod conclusion;
pub mod embed;
pub mod notifier;
pub mod template;
pub mod webhook;

pub use notifier::DiscordNotifier;
pub use webhook::{
    parse_webhook_url, Client, ClientBuildError, ParseWebhookUrlError, ParsedWebhookUrl,
};
