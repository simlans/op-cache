use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::error::Error;
use crate::protocol::{read_message, write_message, Request, Response};

use super::cache::Cache;

pub struct Server {
    config: Config,
    cache: Arc<Cache>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        let cache = Arc::new(Cache::new(config.max_entries, config.ttl_seconds));

        Self { config, cache }
    }

    pub async fn run(&self) -> Result<(), Error> {
        // Remove existing socket if present
        let _ = std::fs::remove_file(&self.config.socket_path);

        // Set restrictive umask before binding to avoid permission race window
        let old_umask = unsafe { libc::umask(0o177) };
        let bind_result = UnixListener::bind(&self.config.socket_path);
        unsafe { libc::umask(old_umask) }; // Restore umask

        let listener = bind_result
            .map_err(|e| Error::Internal(format!("failed to bind socket: {}", e)))?;

        // Verify permissions (belt and suspenders)
        if let Err(e) = std::fs::set_permissions(
            &self.config.socket_path,
            std::fs::Permissions::from_mode(0o600),
        ) {
            let _ = std::fs::remove_file(&self.config.socket_path);
            return Err(Error::Internal(format!("failed to set socket permissions: {}", e)));
        }

        info!("daemon listening on {:?}", self.config.socket_path);

        // Setup shutdown signal
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_tx = Arc::new(shutdown_tx);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let cache = Arc::clone(&self.cache);
                            let mut conn_shutdown_rx = shutdown_rx.clone();
                            let conn_shutdown_tx = Arc::clone(&shutdown_tx);

                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(
                                    stream,
                                    cache,
                                    &mut conn_shutdown_rx,
                                    conn_shutdown_tx,
                                ).await {
                                    debug!("connection error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("shutdown signal received");
                        break;
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("ctrl-c received, shutting down");
                    let _ = shutdown_tx.send(true);
                    break;
                }
            }
        }

        // Cleanup socket
        let _ = std::fs::remove_file(&self.config.socket_path);
        let _ = std::fs::remove_file(self.config.pid_path());

        Ok(())
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    cache: Arc<Cache>,
    shutdown_rx: &mut watch::Receiver<bool>,
    shutdown_tx: Arc<watch::Sender<bool>>,
) -> Result<(), Error> {
    loop {
        tokio::select! {
            result = read_message::<_, Request>(&mut stream) => {
                let request = match result {
                    Ok(req) => req,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(Error::Protocol(e.to_string()));
                    }
                };

                debug!("received request: {:?}", request);

                let response = handle_request(&request, &cache).await;
                let is_shutdown = matches!(request, Request::Shutdown);

                write_message(&mut stream, &response).await?;

                if is_shutdown {
                    info!("shutdown request received, signaling main loop");
                    let _ = shutdown_tx.send(true);
                    return Ok(());
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn handle_request(request: &Request, cache: &Cache) -> Response {
    match request {
        Request::Get { key } => {
            if let Some(value) = cache.get(key).await {
                Response::Hit { value }
            } else {
                Response::Miss
            }
        }
        Request::Set { key, value } => {
            cache.insert(key, value.clone()).await;
            Response::Stored
        }
        Request::GetFile { key } => {
            if let Some((path, _)) = cache.get_file(key).await {
                Response::FileHit { path }
            } else {
                Response::Miss
            }
        }
        Request::SetFile { key, path } => {
            cache.insert_file(key, path.clone()).await;
            Response::FileStored
        }
        Request::Ping => Response::Pong,
        Request::ClearCache => {
            cache.clear();
            Response::CacheCleared
        }
        Request::GetStats => Response::Stats(cache.stats()),
        Request::Shutdown => Response::ShuttingDown,
    }
}
