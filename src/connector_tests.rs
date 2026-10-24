//! Kit side of the framed connector protocol (step 5.6), against small fake
//! hosts on a Unix socket. The real host is tested in fenrir
//! (`tests/unit/infra/db/connector_tests.rs`).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::token_provider::{ServiceTokenLease, ServiceTokenProvider};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixListener;

fn socket_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fmk-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("c.sock")
}

fn client(path: &std::path::Path, framed: bool) -> DbConnectorClient {
    let tokens =
        ServiceTokenProvider::new(ServiceTokenLease::new("read-token", None, None, None), None);
    DbConnectorClient::with_parts(
        ConnectorEndpoint::Ipc {
            path: path.display().to_string(),
        },
        tokens,
        framed,
    )
    .with_timeout(Duration::from_millis(800))
}

fn read_cmd(sql: &str) -> DbConnectorCommand {
    DbConnectorCommand::Prepared {
        statement: sql.to_string(),
        params: Vec::new(),
    }
}

/// What the fake host does with the n-th request it reads (0-based, over
/// all connections).
#[derive(Clone, Copy)]
enum Behaviour {
    Answer,
    CloseWithoutAnswer,
    Silent,
}

struct FakeHost {
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeHost {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Option<DbConnectorRequest> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.ok()?;
    let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut body).await.ok()?;
    serde_json::from_slice(&body).ok()
}

async fn write_frame<S: AsyncWrite + Unpin>(stream: &mut S, response: &DbConnectorResponse) {
    let body = serde_json::to_vec(response).unwrap();
    let mut frame = (body.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    let _ = stream.write_all(&frame).await;
}

/// A framed host: checks the preamble, then answers frames per `plan`
/// (default: answer), echoing the token so tests can see which was used.
fn framed_host(path: &std::path::Path, plan: Vec<Behaviour>) -> FakeHost {
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path).unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let (c, r) = (Arc::clone(&connections), Arc::clone(&requests));
    let plan = Arc::new(plan);
    let task = tokio::spawn(async move {
        // Connection tasks live in the set: aborting the host ends them all,
        // like a host restart.
        let mut set = tokio::task::JoinSet::new();
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            c.fetch_add(1, Ordering::SeqCst);
            let r = Arc::clone(&r);
            let plan = Arc::clone(&plan);
            set.spawn(async move {
                let mut preamble = vec![0u8; CONNECTOR_FRAMED_PREAMBLE.len()];
                if stream.read_exact(&mut preamble).await.is_err()
                    || preamble != CONNECTOR_FRAMED_PREAMBLE
                {
                    return;
                }
                while let Some(request) = read_frame(&mut stream).await {
                    let n = r.fetch_add(1, Ordering::SeqCst);
                    match plan.get(n).copied().unwrap_or(Behaviour::Answer) {
                        Behaviour::Answer => {
                            let response =
                                DbConnectorResponse::ok(vec![DbConnectorResultView::Command {
                                    tag: request.token,
                                }]);
                            write_frame(&mut stream, &response).await;
                        }
                        Behaviour::CloseWithoutAnswer => return,
                        Behaviour::Silent => {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            return;
                        }
                    }
                }
            });
        }
    });
    FakeHost {
        connections,
        requests,
        task,
    }
}

fn tag(response: &DbConnectorResponse) -> String {
    match response.results.as_deref() {
        Some([DbConnectorResultView::Command { tag }]) => tag.clone(),
        other => panic!("unexpected results {other:?}"),
    }
}

#[tokio::test]
async fn framed_calls_reuse_one_connection() {
    let path = socket_path("reuse");
    let host = framed_host(&path, Vec::new());
    let client = client(&path, true);
    for _ in 0..20 {
        let response = client
            .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
            .await
            .unwrap();
        assert_eq!(tag(&response), "read-token");
    }
    assert_eq!(host.requests.load(Ordering::SeqCst), 20);
    assert_eq!(host.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_calls_use_a_small_pool() {
    let path = socket_path("parallel");
    let host = framed_host(&path, Vec::new());
    let client = Arc::new(client(&path, true));
    for _ in 0..5 {
        let calls = (0..4).map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                client
                    .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
                    .await
                    .unwrap()
            })
        });
        for call in calls {
            call.await.unwrap();
        }
    }
    assert_eq!(host.requests.load(Ordering::SeqCst), 20);
    let connections = host.connections.load(Ordering::SeqCst);
    assert!(connections <= 4, "{connections} connections for 4 callers");
}

#[tokio::test]
async fn a_session_holds_one_connection_for_all_its_calls() {
    let path = socket_path("session");
    let host = framed_host(&path, Vec::new());
    let client = client(&path, true);
    {
        let mut session = client.session();
        for _ in 0..3 {
            session
                .execute(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
                .await
                .unwrap();
        }
    }
    // A second, parallel session would need another connection; this one
    // reuses the connection the first session returned.
    client
        .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
        .await
        .unwrap();
    assert_eq!(host.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reconnects_after_the_host_restarted() {
    let path = socket_path("restart");
    let client = client(&path, true);
    let host = framed_host(&path, Vec::new());
    client
        .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
        .await
        .unwrap();
    drop(host);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let host = framed_host(&path, Vec::new());
    // The pooled connection is dead; reads and writes both reconnect.
    client
        .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
        .await
        .unwrap();
    assert_eq!(host.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_read_is_retried_once_when_the_host_closes_before_answering() {
    let path = socket_path("read-retry");
    let host = framed_host(
        &path,
        vec![Behaviour::Answer, Behaviour::CloseWithoutAnswer],
    );
    let client = client(&path, true);
    client
        .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
        .await
        .unwrap();
    client
        .execute_async(read_cmd("SELECT 2"), DbConnectorIntent::Read, None, None)
        .await
        .unwrap();
    assert_eq!(host.requests.load(Ordering::SeqCst), 3);
    assert_eq!(host.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_write_is_never_sent_twice() {
    let path = socket_path("write-once");
    // The write token comes from the cache (no control plane in the test).
    let host = framed_host(
        &path,
        vec![Behaviour::Answer, Behaviour::CloseWithoutAnswer],
    );
    let client = client(&path, true);
    *client.cached_write_token.lock().unwrap() = Some(CachedToken {
        token: "write-token".into(),
        expires_at: Instant::now() + Duration::from_secs(60),
    });
    client
        .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
        .await
        .unwrap();
    let result = client
        .execute_async(
            read_cmd("UPDATE t SET a = 1"),
            DbConnectorIntent::Write,
            None,
            None,
        )
        .await;
    assert!(result.is_err());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        host.requests.load(Ordering::SeqCst),
        2,
        "the write went out once"
    );
}

#[tokio::test]
async fn a_silent_host_times_out() {
    let path = socket_path("timeout");
    let _host = framed_host(&path, vec![Behaviour::Silent]);
    let client = client(&path, true);
    let started = Instant::now();
    let result = client
        .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
        .await;
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
    // The timed-out connection is not reused.
    assert!(client.checkout().is_none());
}

#[tokio::test]
async fn data_modifying_ctes_get_the_write_token() {
    let path = socket_path("cte-token");
    let _host = framed_host(&path, Vec::new());
    let client = client(&path, true);
    *client.cached_write_token.lock().unwrap() = Some(CachedToken {
        token: "write-token".into(),
        expires_at: Instant::now() + Duration::from_secs(60),
    });
    let read = client
        .execute_async(
            read_cmd("WITH x AS (SELECT 1) SELECT * FROM x"),
            DbConnectorIntent::Read,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(tag(&read), "read-token");
    let cte = client
        .execute_async(
            read_cmd("WITH x AS (UPDATE t SET a = 1 RETURNING id) SELECT * FROM x"),
            DbConnectorIntent::Read,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(tag(&cte), "write-token");
}

/// A host without framing (the one-shot protocol of earlier hosts).
#[tokio::test]
async fn legacy_mode_works_against_a_one_shot_host() {
    let path = socket_path("legacy");
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&connections);
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            c.fetch_add(1, Ordering::SeqCst);
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.unwrap();
            let request: DbConnectorRequest = serde_json::from_slice(&buf).unwrap();
            let response = DbConnectorResponse::ok(vec![DbConnectorResultView::Command {
                tag: request.token,
            }]);
            stream
                .write_all(&serde_json::to_vec(&response).unwrap())
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    let client = client(&path, false);
    for _ in 0..3 {
        let response = client
            .execute_async(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
            .await
            .unwrap();
        assert_eq!(tag(&response), "read-token");
    }
    // The blocking API of earlier kits speaks the same protocol.
    let path_for_sync = path.clone();
    let sync = tokio::task::spawn_blocking(move || {
        client
            .execute(read_cmd("SELECT 1"), DbConnectorIntent::Read, None, None)
            .map(|response| (response, path_for_sync))
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(tag(&sync.0), "read-token");
    assert_eq!(connections.load(Ordering::SeqCst), 4);
    task.abort();
}
