// gcit binary entry point.
//
// The `run` subcommand routes to `gcit::flow::run_daemon` which owns
// the supervisor's main select! loop; every other subcommand routes
// through `gcit::cli::*` and returns an `ExitCode`. Global flags
// (--config, --log-filter, --control-socket): CLI flag wins over
// every other source. `--config` falls back to the XDG path under
// non-root euids and to `/etc/gcit/config.toml` otherwise; the
// per-flag doc comments name the exact rules.

use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::parser::ValueSource;
use clap::{ArgGroup, CommandFactory, FromArgMatches, Parser};

use gcit::cli;
use gcit::cli::exit;
use gcit::systemd::InstallScope;

// Compose the `gcit --version` string at compile time. Combines the
// cargo package version with the short git SHA emitted by
// `vergen-gix` in `build.rs`. When the source tree is not a git repo
// (e.g. extracted from a tarball), `VERGEN_GIT_SHA` resolves to
// vergen's "VERGEN_IDEMPOTENT_OUTPUT" placeholder rather than a
// build-time hash; the operator still gets a usable version string
// without breaking `cargo build` outside a git checkout.
//
// Non-doc `//` comment so the const's prose does NOT render in
// `gcit --help` (a `///` here would attach to GCIT_VERSION_STRING
// AND merge with the next adjacent `///` block, displacing the
// binary description).
const GCIT_VERSION_STRING: &str =
    concat!(env!("CARGO_PKG_VERSION"), " (", env!("VERGEN_GIT_SHA"), ")",);

/// gcit -- Poll git repos, dispatch GitHub Actions workflows, post Discord/mail notifications.
///
/// Linux + systemd only. See https://github.com/likewhatevs/gcit for documentation.
#[derive(Parser, Debug)]
#[command(name = "gcit", version = GCIT_VERSION_STRING)]
struct Cli {
    /// Path to the gcit configuration file.
    ///
    /// Default resolution when `--config` is not passed:
    ///   * `gcit install --user` / `gcit uninstall --user`: always
    ///     XDG path (see below) — the install is explicitly user-scoped.
    ///   * Any subcommand invoked by a non-root euid: XDG path. A
    ///     non-root shell running `gcit run --foreground`, `gcit check`,
    ///     `gcit status`, etc. without `--config` defaults to the
    ///     operator's user-scope config rather than the system one.
    ///   * Otherwise (root euid + non-`--user` install): the bottom
    ///     default `/etc/gcit/config.toml`.
    ///
    /// XDG path: `$XDG_CONFIG_HOME/gcit/config.toml` if
    /// `$XDG_CONFIG_HOME` is set, else `$HOME/.config/gcit/config.toml`.
    /// When neither env var is set the bottom default is used so
    /// resolution always succeeds.
    ///
    /// Override here for ad-hoc validation or testing — an explicit
    /// `--config` always wins regardless of euid or scope.
    #[arg(long, global = true, default_value = "/etc/gcit/config.toml")]
    config: PathBuf,

    /// Tracing filter (per `tracing_subscriber::EnvFilter`). The CLI
    /// flag is the only operative source: the config-file
    /// `[log] filter` field is parsed but not currently wired into
    /// log init, and gcit does not honour any `RUST_LOG`-style env
    /// var. When unset, gcit uses its built-in default
    /// (`info,gcit=debug`).
    #[arg(long, global = true)]
    log_filter: Option<String>,

    /// Path to the daemon's Unix control socket. When unset, `gcit
    /// reload`/`status`/`trigger` derive a default from
    /// `$XDG_RUNTIME_DIR/gcit/control.sock` for user installs and
    /// `/run/gcit/control.sock` for system installs. Pass explicitly
    /// to talk to a non-default socket.
    #[arg(long, global = true)]
    control_socket: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Run the gcit daemon. The daemon polls every configured flow,
    /// dispatches workflow runs on SHA changes, monitors them to
    /// completion, and posts notifications. Pass `--foreground` for
    /// development to log to stderr instead of journald.
    Run(RunArgs),
    /// Validate the configuration file. Exits 0 if the config parses,
    /// validates, and every credential is resolvable; exits 78
    /// (EX_CONFIG) on any error. Prints every error in one pass so
    /// the operator can fix multiple issues per edit.
    Check,
    /// Install gcit's systemd units, config, and manifest. `--user`
    /// or `--system` is required. Refuses silent overwrite without
    /// `--force`. With `--dry-run`, renders the systemd service unit
    /// to stdout without writing files, creating users, or invoking
    /// daemon-reload.
    Install(InstallArgs),
    /// Reverse a prior `gcit install` using the install manifest.
    /// `--user` or `--system` is required.
    Uninstall(UninstallArgs),
    /// Send `Reload` over the control socket. Equivalent to SIGHUP
    /// to the daemon process.
    Reload,
    /// Per-flow status snapshot via the control socket.
    Status(StatusArgs),
    /// Manually fire a flow's dispatch path. With `--dry-run`,
    /// returns the rendered payload(s) without contacting GitHub or
    /// any notifier.
    Trigger(TriggerArgs),
    /// Compile a standalone template file using the same template
    /// rules and namespaces (`{{flow.*}}`, `{{source.*}}`,
    /// `{{action.*}}`, `{{run.*}}`, `{{gcit.*}}`) the daemon uses at
    /// notification time. On success the rendered output is printed
    /// to stdout; on compile or render failure exits 65 (EX_DATAERR)
    /// with the underlying error on stderr.
    ValidateTemplate(ValidateTemplateArgs),
    /// Print shell completion script to stdout. Pipe the output to
    /// your shell's completion directory.
    Completions(CompletionsArgs),
}

#[derive(clap::Args, Debug)]
struct RunArgs {
    /// Run in the foreground with stderr logging. Without this flag,
    /// `gcit run` initializes the journald layer and refuses to start
    /// when journald is unreachable. Foreground mode is for development
    /// and `--user` invocations where the operator reads stderr directly.
    #[arg(long)]
    foreground: bool,
}

#[derive(clap::Args, Debug)]
#[command(group = ArgGroup::new("install_scope").required(true).args(["user", "system"]))]
struct InstallArgs {
    /// Install for the current user (writes under XDG paths).
    #[arg(long)]
    user: bool,
    /// Install system-wide (writes under /etc/systemd/system + /etc/gcit).
    #[arg(long)]
    system: bool,
    /// Render the systemd service unit to stdout without writing files,
    /// creating users, or running daemon-reload.
    #[arg(long)]
    dry_run: bool,
    /// Skip the path-preview confirmation prompt (CI use).
    #[arg(long)]
    non_interactive: bool,
    /// Overwrite existing managed files.
    #[arg(long)]
    force: bool,
}

#[derive(clap::Args, Debug)]
#[command(group = ArgGroup::new("uninstall_scope").required(true).args(["user", "system"]))]
struct UninstallArgs {
    /// Uninstall the current user's install (removes files under XDG paths).
    #[arg(long)]
    user: bool,
    /// Uninstall the system-wide install (removes files under /etc/systemd/system + /etc/gcit).
    #[arg(long)]
    system: bool,
    /// Remove operator-modified files (manifest sha mismatch).
    #[arg(long)]
    force: bool,
}

#[derive(clap::Args, Debug)]
struct StatusArgs {
    /// Limit output to a single flow.
    flow: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = cli::status::Format::Text)]
    format: cli::status::Format,
}

#[derive(clap::Args, Debug)]
struct TriggerArgs {
    /// Flow name to fire.
    flow: String,
    /// Render the dispatch payload(s) without contacting GitHub or
    /// any notifier.
    #[arg(long)]
    dry_run: bool,
}

#[derive(clap::Args, Debug)]
struct ValidateTemplateArgs {
    /// Path to the template file. The file's contents are read
    /// verbatim — no TOML wrapping; just the raw template string the
    /// daemon would render at notification time.
    #[arg(value_name = "FILE")]
    template: PathBuf,

    /// Surface this template targets. When set, validate-template
    /// records the surface in its output header so operators
    /// reviewing CI logs see which destination they verified. The
    /// underlying probe context is shared across surfaces today;
    /// surface-specific checks (e.g. Discord's per-job key set,
    /// local_mail's body cap) hang off this flag in future
    /// iterations.
    #[arg(long, value_enum)]
    kind: Option<cli::validate_template::Kind>,
}

#[derive(clap::Args, Debug)]
struct CompletionsArgs {
    /// Target shell. `clap_complete::Shell` natively supports bash,
    /// elvish, fish, powershell, and zsh.
    #[arg(value_enum)]
    shell: clap_complete::Shell,
}

fn main() -> ExitCode {
    // SAFETY: listen_fds_with_names_and_unset_env mutates the process
    // environment (removes LISTEN_PID, LISTEN_FDS, LISTEN_FDNAMES per
    // sd-notify lib.rs:412-421). The function's safety contract
    // requires invocation before threads are spawned, in particular
    // before any tokio runtime initialization. We do that here, ahead
    // of both the CryptoProvider install (single-threaded) and
    // Runtime::new() below. The collected (fd, name) pairs are
    // forwarded to the daemon (`gcit run`); other subcommands ignore
    // them because LISTEN_PID is not set in their environment in any
    // realistic invocation. The unset is cheap and prevents accidental
    // inheritance to spawned helper processes.
    //
    // The fds returned here are O_CLOEXEC and remain open; the
    // daemon entry accepts this collection as input rather than
    // re-querying the environment (which is now unset).
    let listen_fds: Vec<(RawFd, String)> = unsafe {
        sd_notify::listen_fds_with_names_and_unset_env()
            .map(|it| it.collect::<Vec<_>>())
            .unwrap_or_default()
    };

    // rustls 0.23 requires explicit CryptoProvider installation before
    // any TLS handshake. Verified at
    // ~/.cargo/registry/.../rustls-0.23.39/src/crypto/mod.rs:227 ->
    // pub fn install_default(self) -> Result<(), Arc<Self>>.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls CryptoProvider already installed; this is a bug");

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("gcit: failed to start tokio runtime: {e}");
            return ExitCode::from(exit::OSERR);
        }
    };

    rt.block_on(async_main(listen_fds))
}

async fn async_main(listen_fds: Vec<(RawFd, String)>) -> ExitCode {
    let (cli, config_path) = match parse_cli_and_resolve_config() {
        Ok(p) => p,
        Err(code) => return code,
    };
    let log_filter = cli.log_filter.clone();
    let control_socket = cli.control_socket.clone();

    match cli.cmd {
        None => {
            eprintln!("gcit: no subcommand. See `gcit --help`.");
            ExitCode::from(exit::USAGE)
        }
        Some(Cmd::Run(args)) => {
            route_run(
                args,
                config_path,
                control_socket.as_deref(),
                listen_fds,
                log_filter.as_deref(),
            )
            .await
        }
        Some(Cmd::Check) => route_check(&config_path, log_filter.as_deref()),
        Some(Cmd::Install(a)) => route_install(a, &config_path, log_filter.as_deref()).await,
        Some(Cmd::Uninstall(a)) => route_uninstall(a, log_filter.as_deref()).await,
        Some(Cmd::Reload) => route_reload(control_socket.as_deref(), log_filter.as_deref()).await,
        Some(Cmd::Status(a)) => {
            route_status(a, control_socket.as_deref(), log_filter.as_deref()).await
        }
        Some(Cmd::Trigger(a)) => {
            route_trigger(a, control_socket.as_deref(), log_filter.as_deref()).await
        }
        Some(Cmd::ValidateTemplate(a)) => route_validate_template(a, log_filter.as_deref()),
        Some(Cmd::Completions(a)) => route_completions(a),
    }
}

/// Parse argv via clap, resolve the effective config path (XDG vs
/// system bottom default), and surface the config-path mismatch hint
/// for non-root config-consuming subcommands. Returns the parsed Cli
/// + the resolved config path, or an ExitCode the caller bubbles up.
///
/// Custom argument-parsing error path: clap's default exit code is 2,
/// but gcit pins EX_USAGE=64 for invalid invocations. For --help /
/// --version (which clap also routes through Error) we keep clap's
/// exit 0 so `gcit --help | foo` etc. still pipe cleanly.
fn parse_cli_and_resolve_config() -> Result<(Cli, PathBuf), ExitCode> {
    let mut cmd = Cli::command();
    let matches = match cmd.try_get_matches_from_mut(std::env::args_os()) {
        Ok(m) => m,
        Err(e) => {
            let kind = e.kind();
            let _ = e.print();
            if matches!(
                kind,
                clap::error::ErrorKind::DisplayHelp
                    | clap::error::ErrorKind::DisplayVersion
                    | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) {
                return Err(ExitCode::from(exit::OK));
            }
            return Err(ExitCode::from(exit::USAGE));
        }
    };
    let config_explicit = matches!(
        matches.value_source("config"),
        Some(ValueSource::CommandLine | ValueSource::EnvVariable),
    );
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(c) => c,
        Err(e) => {
            let _ = e.print();
            return Err(ExitCode::from(exit::USAGE));
        }
    };
    let config_path = resolve_effective_config_path(&cli, config_explicit);
    emit_config_mismatch_hint_if_applicable(&cli, config_explicit, &config_path);
    Ok((cli, config_path))
}

/// User-scope config default: when --config wasn't passed explicitly,
/// override the bottom default `/etc/gcit/config.toml` with the XDG
/// path in two cases. (1) Install/Uninstall called with `--user`: the
/// install is explicitly user-scoped regardless of who runs it. (2)
/// Any non-install subcommand invoked by a non-root euid: a user
/// shell running `gcit run --foreground` / `gcit check` / `gcit status`
/// etc. without --config defaults to the operator's XDG config rather
/// than the system one. Explicit --config passes through unchanged.
/// `user_default_config` returns None when neither XDG_CONFIG_HOME
/// nor HOME are set; we keep the system bottom default in that case.
fn resolve_effective_config_path(cli: &Cli, config_explicit: bool) -> PathBuf {
    // SAFETY: geteuid() is async-signal-safe and always succeeds.
    let euid_is_root = unsafe { libc::geteuid() } == 0;
    let user_install_scope = matches!(
        cli.cmd.as_ref(),
        Some(Cmd::Install(InstallArgs { user: true, .. }))
            | Some(Cmd::Uninstall(UninstallArgs { user: true, .. })),
    );
    if !config_explicit && (user_install_scope || !euid_is_root) {
        user_default_config().unwrap_or_else(|| cli.config.clone())
    } else {
        cli.config.clone()
    }
}

/// Config-path mismatch hint: when the non-root-euid swap picked the
/// XDG path BUT that path does not exist, AND the system bottom
/// default `/etc/gcit/config.toml` DOES exist, an operator running
/// `gcit check` as themselves to validate a system install would
/// otherwise see a "no such file" error against an XDG path they did
/// not configure. The swap is silent; this hint makes the mismatch
/// visible. Skip for explicit --config (operator is driving) and for
/// `--user` install/uninstall (XDG path not existing is normal — the
/// install is about to create it).
///
/// Only fires for subcommands that actually load the config — `Run`,
/// `Check`, `Install`. The control-socket subcommands (`Reload`,
/// `Status`, `Trigger`) talk to the daemon, not to the config file;
/// `ValidateTemplate` reads only the template; `Completions` writes
/// shell-completion scripts. Emitting the hint for those would be
/// noise.
fn emit_config_mismatch_hint_if_applicable(cli: &Cli, config_explicit: bool, config_path: &Path) {
    let euid_is_root = unsafe { libc::geteuid() } == 0;
    let user_install_scope = matches!(
        cli.cmd.as_ref(),
        Some(Cmd::Install(InstallArgs { user: true, .. }))
            | Some(Cmd::Uninstall(UninstallArgs { user: true, .. })),
    );
    let swap_fired = !config_explicit && !euid_is_root && !user_install_scope;
    let config_consuming = matches!(
        cli.cmd.as_ref(),
        Some(Cmd::Run(_)) | Some(Cmd::Check) | Some(Cmd::Install(_))
    );
    if config_consuming
        && swap_fired
        && !config_path.exists()
        && Path::new("/etc/gcit/config.toml").exists()
    {
        eprintln!(
            "gcit: no user-scope config at {} (you are not running as root, so gcit defaulted \
             to the user-scope path); a system-scope config exists at /etc/gcit/config.toml — \
             pass `--config /etc/gcit/config.toml` to validate it, or run as root.",
            config_path.display(),
        );
    }
}

/// Initialize tracing. `gcit run` is the only subcommand that selects
/// between foreground (stderr) and daemon (journald-only) layers;
/// every other subcommand prints to stderr and uses foreground=true.
/// A bad --log-filter is EX_CONFIG=78 — the only way to reach this
/// branch with an invalid value is an operator typo on the CLI.
fn init_log_or_fail(log_filter: Option<&str>, foreground: bool) -> Result<(), ExitCode> {
    if let Err(e) = gcit::log::init(log_filter, foreground) {
        eprintln!("gcit: log init failed: {}", e);
        return Err(ExitCode::from(exit::CONFIG));
    }
    Ok(())
}

async fn route_run(
    args: RunArgs,
    config_path: PathBuf,
    control_socket: Option<&Path>,
    listen_fds: Vec<(RawFd, String)>,
    log_filter: Option<&str>,
) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, args.foreground) {
        return code;
    }
    let control_socket = resolve_socket(control_socket);
    let params = gcit::flow::supervisor::DaemonParams {
        config_path,
        default_control_socket: control_socket,
        listen_fds,
    };
    match gcit::flow::run_daemon(params).await {
        Ok(()) => ExitCode::from(exit::OK),
        Err(e) => {
            eprintln!("gcit run: {e}");
            ExitCode::from(exit::SOFTWARE)
        }
    }
}

fn route_check(config_path: &Path, log_filter: Option<&str>) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, true) {
        return code;
    }
    cli::check::run(config_path)
}

async fn route_install(a: InstallArgs, config_path: &Path, log_filter: Option<&str>) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, true) {
        return code;
    }
    let scope = pick_scope(a.user, a.system);
    cli::install::run(config_path, scope, !a.non_interactive, a.force, a.dry_run).await
}

async fn route_uninstall(a: UninstallArgs, log_filter: Option<&str>) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, true) {
        return code;
    }
    let scope = pick_scope(a.user, a.system);
    cli::uninstall::run(scope, a.force).await
}

async fn route_reload(control_socket: Option<&Path>, log_filter: Option<&str>) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, true) {
        return code;
    }
    let path = resolve_socket(control_socket);
    cli::reload::run(&path).await
}

async fn route_status(
    a: StatusArgs,
    control_socket: Option<&Path>,
    log_filter: Option<&str>,
) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, true) {
        return code;
    }
    let path = resolve_socket(control_socket);
    cli::status::run(&path, a.flow, a.format).await
}

async fn route_trigger(
    a: TriggerArgs,
    control_socket: Option<&Path>,
    log_filter: Option<&str>,
) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, true) {
        return code;
    }
    let path = resolve_socket(control_socket);
    cli::trigger::run(&path, a.flow, a.dry_run).await
}

fn route_validate_template(a: ValidateTemplateArgs, log_filter: Option<&str>) -> ExitCode {
    if let Err(code) = init_log_or_fail(log_filter, true) {
        return code;
    }
    cli::validate_template::run(&a.template, a.kind)
}

/// Completions go to stdout. The hint to stderr tells operators where
/// to redirect the output — distinct stream so the hint never
/// contaminates the generated script when piped to a file.
fn route_completions(a: CompletionsArgs) -> ExitCode {
    let mut cmd = Cli::command();
    clap_complete::generate(a.shell, &mut cmd, "gcit", &mut std::io::stdout());
    eprintln!(
        "gcit: {} completion script written to stdout. {}",
        a.shell,
        completions_install_hint(a.shell),
    );
    ExitCode::from(exit::OK)
}

/// Per-shell install hint for `gcit completions <SHELL>`. The hint
/// matches each shell's idiomatic completion path so zsh/fish/elvish/
/// powershell operators are not mis-led by a bash-specific example.
///
/// Paths chosen from the upstream `clap_complete::Shell` docs and
/// each shell's documented completion-load behaviour:
///   - bash: XDG-compliant per-user dir loaded by bash-completion
///   - zsh: any directory on `$fpath`; the canonical user-level path
///     is the per-site_functions XDG dir but operators routinely
///     write to a directory listed via `fpath+=(...)` in `.zshrc`,
///     so the hint surfaces the requirement rather than picking one
///   - fish: per-user completions dir in `$XDG_CONFIG_HOME/fish`
///   - elvish: rc file source line — elvish has no auto-load dir
///   - powershell: dot-source from `$PROFILE`
fn completions_install_hint(shell: clap_complete::Shell) -> &'static str {
    match shell {
        clap_complete::Shell::Bash => {
            "Redirect to ~/.local/share/bash-completion/completions/gcit \
             (per-user) or /etc/bash_completion.d/gcit (system)."
        }
        clap_complete::Shell::Zsh => {
            "Redirect to a directory on $fpath named _gcit; for example \
             ~/.zsh/completions/_gcit then add `fpath+=(~/.zsh/completions)` \
             before `compinit` in ~/.zshrc."
        }
        clap_complete::Shell::Fish => "Redirect to ~/.config/fish/completions/gcit.fish.",
        clap_complete::Shell::Elvish => {
            "Redirect to ~/.config/elvish/lib/gcit.elv and add \
             `use gcit` to ~/.config/elvish/rc.elv."
        }
        clap_complete::Shell::PowerShell => {
            "Redirect to a .ps1 file (e.g. ~/.config/powershell/gcit.ps1) \
             and dot-source it from $PROFILE."
        }
        // `Shell` is #[non_exhaustive] in clap_complete; future
        // variants get a generic instruction so the hint never
        // misleads.
        _ => "Redirect to your shell's completion-load path.",
    }
}

/// Map the (user, system) clap flags to InstallScope. `user` and
/// `system` form a required mutex via clap ArgGroup, so exactly one
/// is true here. The fall-through is unreachable — leave a panic so
/// any future regression of the ArgGroup becomes loud rather than
/// silently picking User.
fn pick_scope(user: bool, system: bool) -> InstallScope {
    if system {
        InstallScope::System
    } else if user {
        InstallScope::User
    } else {
        unreachable!(
            "clap ArgGroup ensures exactly one of --user/--system is set; this branch is impossible"
        )
    }
}

/// Default user-scope config path when --config was not passed. Uses
/// `$XDG_CONFIG_HOME/gcit/config.toml` when XDG_CONFIG_HOME is set,
/// or `$HOME/.config/gcit/config.toml` as the canonical fallback.
fn user_default_config() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        let p = PathBuf::from(xdg);
        if !p.as_os_str().is_empty() {
            return Some(p.join("gcit").join("config.toml"));
        }
    }
    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    if home.as_os_str().is_empty() {
        return None;
    }
    Some(home.join(".config").join("gcit").join("config.toml"))
}

/// Default control-socket path when the operator did not pass
/// --control-socket. When XDG_RUNTIME_DIR is set, always use the
/// user-scope path even if the socket file does not exist yet — a
/// `gcit` client started before the daemon must still reach the
/// user-scope socket once the daemon comes up. Only fall back to
/// /run/gcit when XDG_RUNTIME_DIR is genuinely unset (system services,
/// root invocations, container/CI runs).
fn resolve_socket(explicit: Option<&Path>) -> PathBuf {
    if let Some(p) = explicit {
        return p.to_path_buf();
    }
    if let Some(rt) = std::env::var_os("XDG_RUNTIME_DIR") {
        let rt = PathBuf::from(rt);
        if !rt.as_os_str().is_empty() {
            return rt.join("gcit").join("control.sock");
        }
    }
    PathBuf::from("/run/gcit/control.sock")
}
