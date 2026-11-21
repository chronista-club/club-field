//! 入口。KDL のスキーマ（`schemas/field.kdl`）と、unison の channel。
//!
//! 1 つの channel が、1 人が 1 つの field に入っている状態に当たる。
//! - `Join` の request で入る
//! - channel が閉じたら出ていく（操作としての「出ていく」は無い）
//! - field が止まったら、こちらから channel を閉じる

use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use unison::ProtocolServer;
use unison::network::server::ServerHandle;
use unison::network::{MessageType, NetworkError, UnisonChannel};

use crate::core::{FieldKey, Snapshot};
use crate::host::Host;
use crate::runtime::Presence;

/// スキーマの正本はファイル。ビルドに焼き込み、discovery で配る
pub const FIELD_KDL: &str = include_str!("../schemas/field.kdl");

pub const PROTOCOL_NAME: &str = "field";
pub const NAMESPACE: &str = "club.chronista.field";
pub const CHANNEL: &str = "presence";
pub const JOIN: &str = "Join";
pub const TICK: &str = "Tick";

/// `Join` の中身
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRequest {
    pub owner: String,
    pub name: String,
}

/// `Join` の応答。入れたら状態、入れなかったら理由
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JoinReply {
    Joined(Snapshot),
    Refused { error: String },
}

impl JoinReply {
    fn refused(reason: &str) -> Self {
        Self::Refused {
            error: reason.to_string(),
        }
    }
}

/// 動いているサーバ
pub struct FieldServer {
    handle: ServerHandle,
}

impl FieldServer {
    /// 待ち受けを始める
    pub async fn start(host: Arc<Host>, listen: &str) -> anyhow::Result<Self> {
        let server =
            ProtocolServer::with_identity(PROTOCOL_NAME, env!("CARGO_PKG_VERSION"), NAMESPACE);
        server.enable_discovery(FIELD_KDL).await?;
        server
            .register_channel(CHANNEL, move |_ctx, stream| {
                let host = host.clone();
                async move { serve_presence(host, Arc::new(UnisonChannel::new(stream))).await }
            })
            .await;
        let handle = Arc::new(server).listener(listen).spawn().await?;
        Ok(Self { handle })
    }

    /// 実際に待ち受けているアドレス
    pub fn local_addr(&self) -> SocketAddr {
        self.handle.local_addr()
    }

    /// 待ち受けをやめる
    pub async fn shutdown(self) {
        if let Err(error) = self.handle.shutdown().await {
            tracing::warn!("待ち受けの終了で問題: {error}");
        }
    }
}

/// channel 1 本ぶんの応対
async fn serve_presence(host: Arc<Host>, channel: Arc<UnisonChannel>) -> Result<(), NetworkError> {
    // tick を stream へ書く係。これを止める = Presence を手放す = 出ていく
    let mut forwarder: Option<tokio::task::JoinHandle<()>> = None;

    let result = loop {
        match channel.recv().await {
            Ok(message) if message.msg_type == MessageType::Request => {
                if message.method != JOIN {
                    tracing::warn!("知らない request: {}", message.method);
                    continue;
                }
                let reply = if forwarder.is_some() {
                    JoinReply::refused("already_joined")
                } else {
                    match join(&host, &message).await {
                        Ok(presence) => {
                            let reply = JoinReply::Joined(presence.snapshot().clone());
                            // 応答を先に書いてから tick を流し始める。
                            // 同じ stream なので、入った人には必ず「状態 → 以後の tick」の順で届く
                            if let Err(error) = channel
                                .send_response(message.id, &message.method, &reply)
                                .await
                            {
                                break Err(error);
                            }
                            forwarder = Some(tokio::spawn(forward(presence, channel.clone())));
                            continue;
                        }
                        Err(reason) => JoinReply::refused(reason),
                    }
                };
                if let Err(error) = channel
                    .send_response(message.id, &message.method, &reply)
                    .await
                {
                    break Err(error);
                }
            }
            Ok(_) => continue,
            Err(error) if error.is_normal_close() => break Ok(()),
            Err(error) => break Err(error),
        }
    };

    if let Some(task) = forwarder.take() {
        task.abort();
    }
    result
}

async fn join(
    host: &Host,
    message: &unison::network::ProtocolMessage,
) -> Result<Presence, &'static str> {
    let request: JoinRequest = message
        .payload_as_value()
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or("bad_request")?;
    let key = FieldKey::new(request.owner, request.name);
    let field = host.get(&key).ok_or("no_such_field")?;
    field.join().await.map_err(|_| "stopped")
}

/// field の tick を、入っている人の stream へ書き続ける。
///
/// `next_tick` は最新の 1 枚だけを返すので、stream への書き込みが詰まっている間に
/// 進んだ tick は、ここで飛ばされる
async fn forward(mut presence: Presence, channel: Arc<UnisonChannel>) {
    while let Some(note) = presence.next_tick().await {
        if channel.send_event(TICK, &note).await.is_err() {
            // 相手がいなくなった
            return;
        }
    }
    // field が止まった。入っていた人に伝わるよう、こちらから閉じる
    let _ = channel.close().await;
}
