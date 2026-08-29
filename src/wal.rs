use serde::{Serialize, Deserialize};
use std::fs::{OpenOptions};
use std::io::{Write, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use tokio::time;
use anyhow::Result;
use crate::state::KvsState;
use crate::config::Config;
use crate::replication::ReplicationService;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub db_id: u32,
    pub table: String,
    pub op: OpType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpType {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct WalWriter {
    state: Arc<KvsState>,
    config: Arc<Config>,
    sender: UnboundedSender<Operation>,
    receiver: Arc<tokio::sync::Mutex<UnboundedReceiver<Operation>>>,
    flush_trigger: Arc<tokio::sync::Notify>,
}

impl WalWriter {
    pub fn new(state: Arc<KvsState>, config: &Config) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let flush_trigger = Arc::new(tokio::sync::Notify::new());
        WalWriter {
            state,
            config: Arc::new(config.clone()),
            sender: tx,
            receiver: Arc::new(tokio::sync::Mutex::new(rx)),
            flush_trigger,
        }
    }

    pub fn send_operation(&self, op: Operation) {
        let _ = self.sender.send(op);
    }

    pub fn flush_now(&self) {
        self.flush_trigger.notify_one();
    }

    pub fn start(&self, rt: &tokio::runtime::Runtime) {
        let state = self.state.clone();
        let config = self.config.clone();
        let receiver = self.receiver.clone();
        let flush_trigger = self.flush_trigger.clone();
        let peer_addr = config.peer.as_ref().and_then(|p| p.address.parse().ok());

        rt.spawn(async move {
            let mut batch = Vec::with_capacity(config.wal.batch_size);
            let mut flush_interval = time::interval(time::Duration::from_secs(config.wal.flush_interval_secs));

            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        if !batch.is_empty() {
                            write_batch(&batch, &config).await;
                            if let Some(addr) = &peer_addr {
                                let _ = ReplicationService::send_batch(&batch, addr);
                            }
                            batch.clear();
                        }
                    }
                    _ = flush_trigger.notified() => {
                        if !batch.is_empty() {
                            write_batch(&batch, &config).await;
                            if let Some(addr) = &peer_addr {
                                let _ = ReplicationService::send_batch(&batch, addr);
                            }
                            batch.clear();
                        }
                    }
                    // Receive from channel
                    maybe_op = async {
                        let mut rx = receiver.lock().await;
                        rx.recv().await
                    } => {
                        if let Some(op) = maybe_op {
                            batch.push(op);
                            if batch.len() >= config.wal.batch_size {
                                write_batch(&batch, &config).await;
                                if let Some(addr) = &peer_addr {
                                    let _ = ReplicationService::send_batch(&batch, addr);
                                }
                                batch.clear();
                            }
                        }
                    }
                }
            }
        });
    }
}

async fn write_batch(batch: &[Operation], config: &Config) {
    let mut groups: std::collections::HashMap<u32, Vec<&Operation>> = std::collections::HashMap::new();
    for op in batch {
        groups.entry(op.db_id).or_default().push(op);
    }

    for (db_id, ops) in groups {
        let dir = PathBuf::from(&config.storage.base_dir).join(format!("db_{}", db_id));
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("Failed to create directory {}: {}", dir.display(), e);
            continue;
        }
        let file_path = dir.join("wal.bin");
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
        {
            Ok(f) => BufWriter::new(f),
            Err(e) => {
                eprintln!("Failed to open WAL file {}: {}", file_path.display(), e);
                continue;
            }
        };

        for op in ops {
            let encoded = match bincode::serialize(op) {
                Ok(bytes) => bytes,
                Err(e) => {
                    eprintln!("Serialization error: {}", e);
                    continue;
                }
            };
            let len = encoded.len() as u32;
            if let Err(e) = file.write_all(&len.to_le_bytes()) {
                eprintln!("Write length error: {}", e);
                break;
            }
            if let Err(e) = file.write_all(&encoded) {
                eprintln!("Write data error: {}", e);
                break;
            }
        }
        if let Err(e) = file.flush() {
            eprintln!("Flush error: {}", e);
        }
        if let Ok(f) = file.into_inner() {
            let _ = f.sync_all();
        }
    }
}