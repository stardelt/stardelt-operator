//! The operator's Prometheus text (stardelt-operator#607 item 6).
//!
//! A port of its own (`METRICS_ADDR`, default `0.0.0.0:8082`), never an ingress: the
//! chart exposes it as a ClusterIP Service with `prometheus.io/*` annotations. One
//! connection, one request, `GET /metrics` only. Hand-rolled because it is 40 lines and
//! an HTTP server dependency would be a second one to keep licensed and patched.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static START: OnceLock<SystemTime> = OnceLock::new();
static LEADER: AtomicBool = AtomicBool::new(false);
static RECONCILE_ERRORS: AtomicU64 = AtomicU64::new(0);

/// The process start time. Called once, first thing in `run_controller`.
pub fn init() {
    START.get_or_init(SystemTime::now);
}

/// Whether this replica holds the leader lease (operator#612). `false` without a
/// `POD_NAME` (a local `cargo run`) — there is no leader then.
pub fn set_leader(is_leader: bool) {
    LEADER.store(is_leader, Ordering::Relaxed);
}

/// One failed reconcile, from [`crate::controller::error_policy`].
pub fn record_reconcile_error() {
    RECONCILE_ERRORS.fetch_add(1, Ordering::Relaxed);
}

fn seconds_between(from: SystemTime, to: SystemTime) -> f64 {
    to.duration_since(from)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64()
}

pub fn render() -> String {
    let start = START.get().copied().unwrap_or_else(SystemTime::now);
    let mut out = String::new();
    out.push_str("# HELP stardelt_operator_build_info Build information.\n# TYPE stardelt_operator_build_info gauge\n");
    out.push_str(&format!(
        "stardelt_operator_build_info{{version=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION")
    ));
    out.push_str("# HELP stardelt_operator_process_start_time_seconds Start time of the process since the Unix epoch.\n# TYPE stardelt_operator_process_start_time_seconds gauge\n");
    out.push_str(&format!(
        "stardelt_operator_process_start_time_seconds {:.3}\n",
        start
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs_f64()
    ));
    out.push_str("# HELP stardelt_operator_uptime_seconds Seconds since this process started.\n# TYPE stardelt_operator_uptime_seconds gauge\n");
    out.push_str(&format!(
        "stardelt_operator_uptime_seconds {:.3}\n",
        seconds_between(start, SystemTime::now())
    ));
    out.push_str("# HELP stardelt_operator_leader 1 when this replica holds the leader lease.\n# TYPE stardelt_operator_leader gauge\n");
    out.push_str(&format!(
        "stardelt_operator_leader {}\n",
        u8::from(LEADER.load(Ordering::Relaxed))
    ));
    out.push_str("# HELP stardelt_operator_reconcile_errors_total Failed reconciles since start.\n# TYPE stardelt_operator_reconcile_errors_total counter\n");
    out.push_str(&format!(
        "stardelt_operator_reconcile_errors_total {}\n",
        RECONCILE_ERRORS.load(Ordering::Relaxed)
    ));
    out
}

/// `(status, content-type, body)` for one request line. Pure, so the test needs no socket.
pub fn response_for(request_line: &str) -> (u16, &'static str, String) {
    let mut parts = request_line.split_whitespace();
    match (parts.next(), parts.next()) {
        (Some("GET"), Some("/metrics")) => {
            (200, "text/plain; version=0.0.4; charset=utf-8", render())
        }
        (Some("GET"), _) => (404, "text/plain; charset=utf-8", "not found\n".to_string()),
        _ => (
            405,
            "text/plain; charset=utf-8",
            "method not allowed\n".to_string(),
        ),
    }
}

/// Serve until the process exits. One request per connection; the body is 8 KiB max.
pub async fn serve(addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr, "metrics listening");
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buffer = [0u8; 8192];
            let Ok(read) = socket.read(&mut buffer).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buffer[..read]);
            let (status, content_type, body) = response_for(request.lines().next().unwrap_or(""));
            let reason = if status == 200 { "OK" } else { "Error" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_text_has_the_series_a_scraper_needs() {
        init();
        set_leader(true);
        record_reconcile_error();
        let text = render();
        assert!(
            text.contains(&format!(
                "stardelt_operator_build_info{{version=\"{}\"}} 1",
                env!("CARGO_PKG_VERSION")
            )),
            "{text}"
        );
        assert!(
            text.contains("# TYPE stardelt_operator_leader gauge"),
            "{text}"
        );
        assert!(text.contains("stardelt_operator_leader 1"), "{text}");
        assert!(
            text.contains("stardelt_operator_reconcile_errors_total 1"),
            "{text}"
        );
        assert!(
            text.contains("stardelt_operator_process_start_time_seconds "),
            "{text}"
        );
        set_leader(false);
    }

    #[test]
    fn only_a_metrics_get_is_answered() {
        assert_eq!(response_for("GET /metrics HTTP/1.1").0, 200);
        assert_eq!(response_for("GET /health HTTP/1.1").0, 404);
        assert_eq!(response_for("POST /metrics HTTP/1.1").0, 405);
        assert!(
            response_for("GET /metrics HTTP/1.1")
                .2
                .contains("stardelt_operator_build_info")
        );
    }
}
