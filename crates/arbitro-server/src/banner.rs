//! Startup banner — a one-shot, human-readable summary of the running
//! configuration, printed unconditionally at boot.
//!
//! Design notes:
//!
//! * **Pure render.** [`render`] takes a [`Config`] plus a [`BannerContext`]
//!   (runtime facts: version, pid, compiled features, active log filter) and
//!   returns a `String`. No I/O, no `cfg` gates inside — which feature flags
//!   are compiled is data in the context — so the function is unit-testable
//!   under every feature combination.
//! * **Printed to stderr.** The tracing subscriber writes logs to stdout, and
//!   with `ARBITRO_LOG_FORMAT=json` that stream is machine-parsed. A banner on
//!   stderr is always visible to the operator and never corrupts a piped or
//!   aggregated log stream.
//! * **ASCII only, no color.** Renders identically in any terminal, Docker
//!   logs, CI capture and log files.
//! * **No secrets.** The auth token and TLS key contents are never printed —
//!   only on/off state and the certificate *path*.

use crate::config::{Config, FsyncPolicy};

/// Left gutter width for section names ("network", "security", ...).
const GUTTER: usize = 8;
/// Column at which field values start (label + dot leaders end here).
const LABEL_COL: usize = 21;
/// Minimum inner width of the box (it grows if a line needs more room).
const MIN_INNER: usize = 68;

/// Runtime facts that accompany the [`Config`] in the banner.
///
/// Kept separate from `Config` so [`render`] stays pure: everything that
/// would otherwise require `env!`, `cfg!` or `std::env` lookups inside the
/// renderer is captured here once, at the call site.
pub struct BannerContext {
    /// Crate version (`CARGO_PKG_VERSION`).
    pub version: String,
    /// Process id.
    pub pid: u32,
    /// `true` for release builds, `false` for debug builds.
    pub release_build: bool,
    /// Cargo features compiled into this binary.
    pub features: Vec<&'static str>,
    /// Effective tracing filter directive at startup.
    pub log_filter: String,
    /// Where the filter came from: `"RUST_LOG"` or `"default"`.
    pub log_filter_source: &'static str,
    /// Log output format: `"text"` or `"json"`.
    pub log_format: &'static str,
    /// `ARBITRO_HEALTH_LISTEN`, when set.
    pub health_listen: Option<String>,
    /// `ARBITRO_METRICS_LISTEN`, when set.
    pub metrics_listen: Option<String>,
    /// Whether the `tls` feature is compiled in.
    pub tls_compiled: bool,
    /// Whether the `cluster` feature is compiled in.
    pub cluster_compiled: bool,
}

impl BannerContext {
    /// Capture the runtime facts for this process from the environment.
    pub fn from_env() -> Self {
        // Mirror main.rs: the initial tracing filter comes from RUST_LOG
        // (EnvFilter::try_from_default_env), falling back to the built-in
        // default. ARBITRO_LOG is only consulted on SIGHUP reload.
        let (log_filter, log_filter_source) = match std::env::var("RUST_LOG") {
            Ok(v) if !v.trim().is_empty() => (v, "RUST_LOG"),
            _ => ("arbitro_server=info".to_string(), "default"),
        };
        let log_format = match std::env::var("ARBITRO_LOG_FORMAT") {
            Ok(v) if v.eq_ignore_ascii_case("json") => "json",
            _ => "text",
        };
        let mut features = Vec::new();
        if cfg!(feature = "tls") {
            features.push("tls");
        }
        if cfg!(feature = "cluster") {
            features.push("cluster");
        }
        if cfg!(feature = "cluster-tls") {
            features.push("cluster-tls");
        }
        if cfg!(feature = "lifecycle_trace") {
            features.push("lifecycle_trace");
        }
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            pid: std::process::id(),
            release_build: !cfg!(debug_assertions),
            features,
            log_filter,
            log_filter_source,
            log_format,
            health_listen: std::env::var("ARBITRO_HEALTH_LISTEN").ok(),
            metrics_listen: std::env::var("ARBITRO_METRICS_LISTEN").ok(),
            tls_compiled: cfg!(feature = "tls"),
            cluster_compiled: cfg!(feature = "cluster"),
        }
    }
}

/// Render the startup banner as a boxed, aligned, ASCII-only summary.
///
/// The returned string has no trailing newline; print with `eprintln!`.
pub fn render(config: &Config, ctx: &BannerContext) -> String {
    // ── Header: wordmark (left) + build metadata (right) ──────────────
    let logo = [
        r"            _     _ _",
        r"  __ _ _ __| |__ (_) |_ _ __ ___",
        r" / _` | '__| '_ \| | __| '__/ _ \",
        r"| (_| | |  | |_) | | |_| | | (_) |",
        r" \__,_|_|  |_.__/|_|\__|_|  \___/",
    ];
    let features = if ctx.features.is_empty() {
        "(none)".to_string()
    } else {
        ctx.features.join(", ")
    };
    let meta = [
        format!("arbitro-server v{}", ctx.version),
        format!(
            "{} build  --  pid {}",
            if ctx.release_build {
                "release"
            } else {
                "debug"
            },
            ctx.pid
        ),
        format!("features: {features}"),
        "high-performance message broker".to_string(),
    ];
    let mut head: Vec<String> = Vec::new();
    head.push(String::new());
    head.push(format!("  {}", logo[0]));
    for i in 0..4 {
        head.push(format!("  {:<36}{}", logo[i + 1], meta[i]));
    }
    head.push(String::new());

    // ── Body: grouped fields ──────────────────────────────────────────
    let mut body: Vec<String> = Vec::new();
    body.push(String::new());

    // network
    let tls_configured = config.tls_cert.is_some();
    let transport = if ctx.tls_compiled && tls_configured {
        "TLS"
    } else {
        "plaintext TCP"
    };
    field(
        &mut body,
        "network",
        "listen",
        &format!("{}  ({})", config.listen_addr, transport),
    );
    let tls_value = match (ctx.tls_compiled, tls_configured) {
        (true, true) => format!(
            "on  (cert: {})",
            config.tls_cert.as_deref().unwrap_or_default()
        ),
        (true, false) => "off".to_string(),
        (false, true) => "OFF -- cert set but 'tls' feature missing".to_string(),
        (false, false) => "off (feature not compiled)".to_string(),
    };
    field(&mut body, "", "tls", &tls_value);
    if let Some(addr) = &ctx.health_listen {
        field(&mut body, "", "health", addr);
    }
    if let Some(addr) = &ctx.metrics_listen {
        field(&mut body, "", "metrics", addr);
    }
    field(
        &mut body,
        "",
        "max frame",
        &human_bytes(config.max_frame_size),
    );
    field(
        &mut body,
        "",
        "max connections",
        &config.max_connections.to_string(),
    );
    let rate = if config.max_ops_per_sec == 0 {
        "unlimited".to_string()
    } else {
        format!("{} frames/sec per conn", config.max_ops_per_sec)
    };
    field(&mut body, "", "rate limit", &rate);
    body.push(String::new());

    // storage
    let data_dir = config
        .data_dir
        .as_deref()
        .unwrap_or("in-memory (no persistence)");
    field(&mut body, "storage", "data dir", data_dir);
    if config.data_dir.is_some() {
        let fsync = match config.fsync_policy {
            FsyncPolicy::Every => "every (fdatasync per write)",
            FsyncPolicy::None => "none (batched flush)",
        };
        field(&mut body, "", "fsync", fsync);
    }
    body.push(String::new());

    // engine
    field(
        &mut body,
        "engine",
        "shards",
        &config.shard_count.to_string(),
    );
    field(
        &mut body,
        "",
        "write buffer",
        &format!("{} frames per conn", config.write_buffer_cap),
    );
    body.push(String::new());

    // cluster
    let clustered = ctx.cluster_compiled && !config.cluster_peers.is_empty();
    if clustered {
        field(&mut body, "cluster", "mode", "clustered (raft)");
        field(
            &mut body,
            "",
            "node id",
            &config.cluster_node_id.to_string(),
        );
        field(&mut body, "", "raft listen", &config.cluster_listen);
        field(
            &mut body,
            "",
            "peers",
            &config.cluster_peers.len().to_string(),
        );
        for (id, addr) in &config.cluster_peers {
            let tag = if *id == config.cluster_node_id {
                "  (this node)"
            } else {
                ""
            };
            body.push(format!(
                "  {:<g$}    - {}@{}{}",
                "",
                id,
                addr,
                tag,
                g = GUTTER
            ));
        }
    } else {
        let mode = if ctx.cluster_compiled {
            "standalone (no peers configured)"
        } else {
            "standalone"
        };
        field(&mut body, "cluster", "mode", mode);
    }
    body.push(String::new());

    // security
    let auth = if config.auth_token.is_some() {
        "token required"
    } else {
        "disabled (open access)"
    };
    field(&mut body, "security", "auth", auth);
    body.push(String::new());

    // logging
    field(
        &mut body,
        "logging",
        "filter",
        &format!("{}  ({})", ctx.log_filter, ctx.log_filter_source),
    );
    field(&mut body, "", "format", ctx.log_format);
    body.push(String::new());

    // ── Assemble the box ──────────────────────────────────────────────
    let inner = head
        .iter()
        .chain(body.iter())
        .map(|l| l.len())
        .max()
        .unwrap_or(0)
        .max(MIN_INNER);
    let border = format!("+{}+", "-".repeat(inner + 2));
    let mut out = String::new();
    out.push_str(&border);
    for line in &head {
        out.push('\n');
        out.push_str(&format!("| {line:<inner$} |"));
    }
    out.push('\n');
    out.push_str(&border);
    for line in &body {
        out.push('\n');
        out.push_str(&format!("| {line:<inner$} |"));
    }
    out.push('\n');
    out.push_str(&border);
    out
}

/// Push one aligned `section  label ........ value` row.
///
/// `gutter` is the section name — pass `""` for continuation rows.
fn field(lines: &mut Vec<String>, gutter: &str, label: &str, value: &str) {
    let mut lab = String::with_capacity(LABEL_COL + 1);
    lab.push_str(label);
    lab.push(' ');
    while lab.len() < LABEL_COL {
        lab.push('.');
    }
    lines.push(format!("  {gutter:<g$}  {lab} {value}", g = GUTTER));
}

/// Format a byte count for humans: exact MiB/KiB when round, raw otherwise.
fn human_bytes(n: usize) -> String {
    const MIB: usize = 1024 * 1024;
    const KIB: usize = 1024;
    if n >= MIB && n.is_multiple_of(MIB) {
        format!("{} MiB", n / MIB)
    } else if n >= KIB && n.is_multiple_of(KIB) {
        format!("{} KiB", n / KIB)
    } else {
        format!("{n} bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn ctx() -> BannerContext {
        BannerContext {
            version: "9.9.9".to_string(),
            pid: 4242,
            release_build: true,
            features: vec!["tls"],
            log_filter: "arbitro_server=info".to_string(),
            log_filter_source: "default",
            log_format: "text",
            health_listen: None,
            metrics_listen: None,
            tls_compiled: true,
            cluster_compiled: false,
        }
    }

    #[test]
    fn standalone_banner_shows_key_fields() {
        let config = Config::default().data_dir("./data");
        let banner = render(&config, &ctx());
        for needle in [
            "arbitro-server v9.9.9",
            "release build  --  pid 4242",
            "features: tls",
            "0.0.0.0:9898  (plaintext TCP)",
            "64 MiB",
            "./data",
            "none (batched flush)",
            "standalone",
            "disabled (open access)",
            "arbitro_server=info  (default)",
            "text",
        ] {
            assert!(banner.contains(needle), "missing {needle:?} in:\n{banner}");
        }
    }

    #[test]
    fn in_memory_when_no_data_dir() {
        let config = Config::default();
        let banner = render(&config, &ctx());
        assert!(banner.contains("in-memory (no persistence)"));
        // No fsync row without persistence.
        assert!(!banner.contains("fsync"));
    }

    #[test]
    fn clustered_banner_lists_peers_and_marks_self() {
        let mut config = Config::default();
        config.cluster_peers = vec![
            (1, "10.0.0.1:9900".to_string()),
            (2, "10.0.0.2:9900".to_string()),
        ];
        config.cluster_node_id = 2;
        let mut c = ctx();
        c.cluster_compiled = true;
        let banner = render(&config, &c);
        assert!(banner.contains("clustered (raft)"));
        assert!(banner.contains("- 1@10.0.0.1:9900"));
        assert!(banner.contains("- 2@10.0.0.2:9900  (this node)"));
        assert!(!banner.contains("standalone"));
    }

    #[test]
    fn secrets_never_printed() {
        let mut config = Config::default();
        config.auth_token = Some("s3cret-token".to_string());
        config.tls_cert = Some("/etc/arbitro/cert.pem".to_string());
        config.tls_key = Some("/etc/arbitro/key.pem".to_string());
        let banner = render(&config, &ctx());
        assert!(!banner.contains("s3cret-token"));
        assert!(banner.contains("token required"));
        // Cert path is fine; the key path/contents are not shown.
        assert!(banner.contains("/etc/arbitro/cert.pem"));
        assert!(!banner.contains("key.pem"));
    }

    #[test]
    fn tls_misconfiguration_is_flagged_when_feature_missing() {
        let mut config = Config::default();
        config.tls_cert = Some("cert.pem".to_string());
        config.tls_key = Some("key.pem".to_string());
        let mut c = ctx();
        c.tls_compiled = false;
        let banner = render(&config, &c);
        assert!(banner.contains("'tls' feature missing"));
        assert!(banner.contains("(plaintext TCP)"));
    }

    #[test]
    fn box_lines_are_uniform_width() {
        let mut config = Config::default().data_dir("./data");
        config.cluster_peers = vec![(1, "127.0.0.1:9900".to_string())];
        let mut c = ctx();
        c.cluster_compiled = true;
        c.health_listen = Some("0.0.0.0:9090".to_string());
        c.metrics_listen = Some("0.0.0.0:9091".to_string());
        let banner = render(&config, &c);
        let widths: Vec<usize> = banner.lines().map(|l| l.len()).collect();
        assert!(
            widths.windows(2).all(|w| w[0] == w[1]),
            "ragged box:\n{banner}"
        );
        assert!(banner.lines().all(|l| l.is_ascii()), "non-ASCII output");
    }

    #[test]
    fn human_bytes_formats() {
        assert_eq!(human_bytes(64 * 1024 * 1024), "64 MiB");
        assert_eq!(human_bytes(512 * 1024), "512 KiB");
        assert_eq!(human_bytes(1000), "1000 bytes");
    }
}
