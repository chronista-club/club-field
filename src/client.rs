//! field に入って tick を受け取る、確かめ用の小さなクライアント。
//!
//! 結合テストと `field watch` が使う。描画などはしない。
//! 証明書を検証しないので、つなげる先は自分のマシンの中（loopback）だけ。

use std::net::SocketAddr;

use unison::ProtocolClient;
use unison::network::{MessageType, UnisonChannel};

use crate::core::{FieldKey, Snapshot};
use crate::protocol::{CHANNEL, JOIN, JoinReply, JoinRequest, TICK};

#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    /// field の側が断った。中身は理由（`schemas/field.kdl` の error）
    #[error("入れなかった: {0}")]
    Refused(String),
    #[error("つながらなかった: {0}")]
    Network(String),
}

/// field に入っている状態
pub struct FieldClient {
    client: ProtocolClient,
    channel: UnisonChannel,
    snapshot: Snapshot,
}

impl FieldClient {
    /// 入る
    pub async fn join(addr: SocketAddr, key: &FieldKey) -> Result<Self, JoinError> {
        let network = |error: &dyn std::fmt::Display| JoinError::Network(error.to_string());

        let client = ProtocolClient::insecure_localhost().map_err(|e| network(&e))?;
        client
            .connect(&addr.to_string())
            .await
            .map_err(|e| network(&e))?;
        let channel = client
            .open_channel(CHANNEL)
            .await
            .map_err(|e| network(&e))?;
        let request = JoinRequest {
            owner: key.owner.clone(),
            name: key.name.clone(),
        };
        let reply: JoinReply = channel
            .request(JOIN, &request)
            .await
            .map_err(|e| network(&e))?;
        match reply {
            JoinReply::Joined(snapshot) => Ok(Self {
                client,
                channel,
                snapshot,
            }),
            JoinReply::Refused { error } => {
                let _ = client.disconnect().await;
                Err(JoinError::Refused(error))
            }
        }
    }

    /// 入った時点の状態
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// 次の tick を待つ。field が止まった・切れたら `None`
    pub async fn next_tick(&mut self) -> Option<Snapshot> {
        loop {
            let message = self.channel.recv().await.ok()?;
            if message.msg_type != MessageType::Event || message.method != TICK {
                continue;
            }
            let value = message.payload_as_value().ok()?;
            return serde_json::from_value(value).ok();
        }
    }

    /// 出ていく
    pub async fn leave(self) {
        let _ = self.channel.close().await;
        let _ = self.client.disconnect().await;
    }
}
