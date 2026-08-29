use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::runtime::Runtime;
use anyhow::Result;
use crate::state::KvsState;
use crate::config::Config;
use crate::wal::Operation;

pub struct ReplicationService {
    state: Arc<KvsState>,
    peer_addr: Option<SocketAddr>,
    local_port: u16,
}

impl ReplicationService {
    pub fn new(state: Arc<KvsState>, config: &Config) -> Self {
        let peer = config.peer.as_ref().and_then(|p| p.address.parse().ok());
        ReplicationService {
            state,
            peer_addr: peer,
            local_port: config.server.replication_port,
        }
    }

    pub fn start(&self, rt: &Runtime) {
        let state = self.state.clone();
        let peer_addr = self.peer_addr.clone();
        let local_port = self.local_port;

        rt.spawn(async move {
            let bind_addr = format!("0.0.0.0:{}", local_port);
            let socket = match TokioUdpSocket::bind(&bind_addr).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Failed to bind replication port: {}", e);
                    return;
                }
            };
            let mut buf = vec![0u8; 65536];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, src)) => {
                        let data = &buf[..len];
                        match bincode::deserialize::<Vec<Operation>>(data) {
                            Ok(ops) => {
                                for op in ops {
                                    match op.op {
                                        crate::wal::OpType::Put { key, value } => {
                                            state.put_raw(op.db_id, &op.table, key, value);
                                        }
                                        crate::wal::OpType::Delete { key } => {
                                            state.remove_raw(op.db_id, &op.table, &key);
                                        }
                                    }
                                }
                            }
                            Err(e) => eprintln!("Replication deserialization error: {}", e),
                        }
                    }
                    Err(e) => eprintln!("UDP receive error: {}", e),
                }
            }
        });
    }

    pub fn send_batch(batch: &[Operation], peer_addr: &SocketAddr) -> Result<()> {
        let encoded = bincode::serialize(batch)?;
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.send_to(&encoded, peer_addr)?;
        Ok(())
    }
}