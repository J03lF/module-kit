//! Auto-refresh of the module token against a small fake control plane on
//! 127.0.0.1 (plain HTTP, one connection per request), with shortened waits.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::env::{ControlPlaneEnvironment, ControlPlaneTlsEnvironment};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicUsize;
use url::Url;

const FRESH: &str = r#"{"token":"fresh-token","scopes":[],"expires_in_seconds":3600}"#;

fn timing() -> RefreshTiming {
    RefreshTiming {
        due_wait: StdDuration::from_millis(100),
        retry_base: StdDuration::from_millis(40),
        retry_cap: StdDuration::from_millis(400),
    }
}

struct FakeControlPlane {
    port: u16,
    hits: Arc<AtomicUsize>,
}

/// Answers the n-th request (1-based) with `answer(n, body)`.
fn fake_control_plane<F>(answer: F) -> FakeControlPlane
where
    F: Fn(usize, &str) -> (u16, &'static str) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let body = read_body(&mut stream);
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            let (status, reply) = answer(n, &body);
            let _ = write!(
                stream,
                "HTTP/1.1 {status} Fake\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{reply}",
                reply.len()
            );
        }
    });
    FakeControlPlane { port, hits }
}

fn read_body(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let text = String::from_utf8_lossy(&buf).to_string();
        if let Some(end) = text.find("\r\n\r\n") {
            let length = text[..end]
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if buf.len() >= end + 4 + length {
                return text[end + 4..].to_string();
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return String::new(),
            Ok(read) => buf.extend_from_slice(&chunk[..read]),
        }
    }
}

fn provider(port: u16, lease: ServiceTokenLease) -> ServiceTokenProvider {
    let env = ControlPlaneEnvironment {
        url: Some(Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap()),
        timeout: StdDuration::from_secs(2),
        retries: 0,
        backoff: StdDuration::from_millis(1),
        tls: ControlPlaneTlsEnvironment::default(),
    };
    let client = ControlPlaneClient::new(&env).unwrap();
    ServiceTokenProvider::with_timing(lease, Some(client), timing())
}

/// A lease the refresh thread treats as due right away.
fn due_lease(token: &str) -> ServiceTokenLease {
    ServiceTokenLease::new(
        token,
        None,
        Some(OffsetDateTime::now_utc() + Duration::seconds(10)),
        None,
    )
}

fn fresh_lease(token: &str) -> ServiceTokenLease {
    ServiceTokenLease::new(
        token,
        None,
        Some(OffsetDateTime::now_utc() + Duration::hours(1)),
        None,
    )
}

fn recorder(provider: &ServiceTokenProvider) -> Arc<Mutex<Vec<TokenRefreshFailure>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    provider.set_refresh_hook(move |failure| sink.lock().unwrap().push(failure.clone()));
    events
}

fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + StdDuration::from_secs(5);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(StdDuration::from_millis(10));
    }
}

#[test]
fn a_refused_token_is_not_retried_until_a_new_one_is_installed() {
    let cp = fake_control_plane(|_, _| (401, r#"{"title":"token expired"}"#));
    let provider = provider(cp.port, due_lease("dead-token"));
    let events = recorder(&provider);

    wait_until("the refusal", || provider.refresh_health().rejected);
    // Several retry caps long: the old loop asked again every few waits.
    thread::sleep(StdDuration::from_millis(900));
    assert_eq!(cp.hits.load(Ordering::SeqCst), 1);

    let health = provider.refresh_health();
    assert_eq!(health.consecutive_failures, 1);
    assert_eq!(
        health.last_failure_kind,
        Some(TokenRefreshFailureKind::Rejected)
    );
    assert_eq!(health.last_status, Some(401));
    assert!(health.last_success.is_none());
    assert!(!health.is_healthy());
    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, TokenRefreshFailureKind::Rejected);
        assert_eq!(events[0].status, Some(401));
        assert_eq!(events[0].retry_in, None);
        assert!(!events[0].message.contains("dead-token"));
    }

    // The on-demand path fails fast instead of asking again.
    assert!(provider.current_token().is_err());
    assert_eq!(cp.hits.load(Ordering::SeqCst), 1);

    provider.install_lease(fresh_lease("new-token"));
    let health = provider.refresh_health();
    assert!(!health.rejected);
    assert_eq!(health.consecutive_failures, 0);
    assert!(health.is_healthy());
    assert_eq!(provider.current_token().unwrap(), "new-token");
}

#[test]
fn an_outage_backs_off_and_recovers() {
    let cp = fake_control_plane(|n, _| if n <= 4 { (503, "{}") } else { (200, FRESH) });
    let started = Instant::now();
    let provider = provider(cp.port, due_lease("old-token"));
    let events = recorder(&provider);

    wait_until("the recovery", || {
        provider.refresh_health().last_success.is_some()
    });
    let elapsed = started.elapsed();
    assert_eq!(cp.hits.load(Ordering::SeqCst), 5);
    // due wait + at least half of 40, 80, 160, 320 ms; a fixed 40 ms retry
    // would be done after ~260 ms.
    assert!(elapsed >= StdDuration::from_millis(400), "{elapsed:?}");

    let events = events.lock().unwrap();
    assert_eq!(events.len(), 4);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.kind, TokenRefreshFailureKind::Status);
        assert_eq!(event.status, Some(503));
        assert_eq!(event.consecutive_failures, i as u32 + 1);
        let retry_in = event.retry_in.unwrap();
        let full = StdDuration::from_millis(40 << i).min(timing().retry_cap);
        assert!(
            retry_in >= full / 2 && retry_in <= full,
            "{i}: {retry_in:?}"
        );
    }

    let health = provider.refresh_health();
    assert_eq!(health.consecutive_failures, 0);
    assert!(!health.rejected);
    assert!(health.is_healthy());
    assert_eq!(provider.token_if_fresh().as_deref(), Some("fresh-token"));
}

#[test]
fn an_unreachable_control_plane_is_a_transport_failure() {
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let provider = provider(port, due_lease("old-token"));
    assert!(provider.current_token().is_err());
    let health = provider.refresh_health();
    assert_eq!(
        health.last_failure_kind,
        Some(TokenRefreshFailureKind::Transport)
    );
    assert!(health.consecutive_failures >= 1);
    assert!(!health.rejected);
}

#[test]
fn a_refused_scoped_token_does_not_mark_the_lease_refused() {
    let cp = fake_control_plane(|_, body| {
        if body.contains("db:write") {
            (403, r#"{"title":"scope not allowed"}"#)
        } else {
            (200, FRESH)
        }
    });
    let provider = provider(cp.port, fresh_lease("module-token"));
    assert!(provider
        .issue_scoped_token(ModuleTokenExchangeRequest::db_write())
        .is_err());
    let health = provider.refresh_health();
    assert!(!health.rejected);
    assert_eq!(health.consecutive_failures, 0);
    assert_eq!(provider.current_token().unwrap(), "module-token");
}

#[test]
fn retry_delay_grows_is_capped_and_jittered() {
    let t = timing();
    assert_eq!(t.retry_delay(1, 0), StdDuration::from_millis(20));
    assert_eq!(t.retry_delay(1, 20), StdDuration::from_millis(40));
    assert_eq!(t.retry_delay(3, 0), StdDuration::from_millis(80));
    assert_eq!(t.retry_delay(3, 80), StdDuration::from_millis(160));
    for failures in [5, 6, 30, u32::MAX] {
        for random in [0, 7, u64::MAX] {
            let delay = t.retry_delay(failures, random);
            assert!(
                delay >= t.retry_cap / 2 && delay <= t.retry_cap,
                "{delay:?}"
            );
        }
    }
    let cap = StdDuration::from_secs(AUTO_REFRESH_RETRY_CAP_SECS);
    let delay = RefreshTiming::default().retry_delay(u32::MAX, u64::MAX);
    assert!(delay >= cap / 2 && delay <= cap, "{delay:?}");
}

#[test]
fn debug_of_a_lease_hides_the_token() {
    let lease = ServiceTokenLease::new("lease-secret-42", None, None, Some(60));
    let shown = format!("{lease:?} {lease:#?}");
    assert!(!shown.contains("lease-secret-42"), "{shown}");
    assert!(shown.contains("ttl_seconds"), "{shown}");
}
