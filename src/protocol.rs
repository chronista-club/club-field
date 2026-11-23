//! 入口。KDL のスキーマ（`schemas/field.kdl`）と、unison の channel。
//!
//! 入った人 1 人につき、道が 2 本:
//! - `presence`（入る人が開く）: `Join` で入り、`Set` で置き換えを頼む。
//!   channel が閉じたら出ていく（操作としての「出ていく」は無い）
//! - `feed`（field が開く）: 要素の変動を `Update` として書き続ける
//!
//! feed を field の側から開くのは、受け取る側が stream を直接読むため。
//! 入る人が開く channel の event は、受け取る側の待ち行列（256 件）が満ちると黙って捨てられる
//! （club-unison の `UnisonChannel`）。field の側から開いた stream にはその待ち行列が無く、
//! 受け取りが遅ければ QUIC の流量制御で送る側が待たされる。待たされている間の変動は、
//! 購読（sync）が要素ごとに最新の値へ畳む。
//!
//! field が止まったら、こちらから feed と presence を閉じる。
//!
//! 待ち受けるのは、[`Host`] に置いてあるもの（field そのもの、または中継）。
//! 中継（[`RelayHandle`]）に入る人にも、同じ道で同じ受け答えをする。
//! presence は 1 本ごとに別のタスクで応対する。中継が、ある人の頼みで上流の返事を待っていても、
//! ほかの人の `Join` / `Set` は待たされない。1 本の presence の中では、頼みを届いた順に 1 つずつ運ぶ
//! （1 人の頼みの順番を保つ）。

use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use unison::ProtocolServer;
use unison::network::context::ConnectionContext;
use unison::network::server::ServerHandle;
use unison::network::{MessageType, NetworkError, ProtocolMessage, UnisonChannel, UnisonStream};

use crate::core::{FieldError, FieldKey, FieldSnapshot};
use crate::host::{Host, Hosted};
use crate::relay::{RelayHandle, RelayPresence};
use crate::runtime::{FieldHandle, Presence};
use crate::sync::{SlotUpdate, SyncError};

/// スキーマの正本はファイル。ビルドに焼き込み、discovery で配る
pub const FIELD_KDL: &str = include_str!("../schemas/field.kdl");

pub const PROTOCOL_NAME: &str = "field";
pub const NAMESPACE: &str = "club.chronista.field";
pub const PRESENCE: &str = "presence";
pub const FEED: &str = "feed";
pub const JOIN: &str = "Join";
pub const SET: &str = "Set";
pub const UPDATE: &str = "Update";

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
    Joined(FieldSnapshot),
    Refused { error: String },
}

/// `Set` の中身
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetRequest {
    pub slot: String,
    pub value: Value,
}

/// `Set` の応答。確定したら変動、しなかったら理由
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SetReply {
    Set(SlotUpdate),
    Refused { error: String },
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
            .register_channel(PRESENCE, move |ctx, stream| {
                let host = host.clone();
                async move { serve_presence(host, ctx, Arc::new(UnisonChannel::new(stream))).await }
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

/// 入っている間の持ち物
struct Joined {
    /// 入った先（置き換えの頼みの行き先）
    door: Hosted,
    /// 変動を feed へ書く係。これを止める = Presence を手放す = 出ていく
    forwarder: tokio::task::JoinHandle<()>,
}

/// 入っている、ということ。手放す（drop する）と出ていく
enum Visit {
    Field(Presence),
    Relay(RelayPresence),
}

impl Visit {
    async fn next(&mut self) -> Option<Vec<SlotUpdate>> {
        match self {
            Visit::Field(presence) => presence.next().await,
            Visit::Relay(presence) => presence.next().await,
        }
    }
}

/// presence 1 本ぶんの応対
async fn serve_presence(
    host: Arc<Host>,
    ctx: Arc<ConnectionContext>,
    channel: Arc<UnisonChannel>,
) -> Result<(), NetworkError> {
    let mut joined: Option<Joined> = None;

    let result = loop {
        let message = match channel.recv().await {
            Ok(message) if message.msg_type == MessageType::Request => message,
            Ok(_) => continue,
            Err(error) if error.is_normal_close() => break Ok(()),
            Err(error) => break Err(error),
        };
        let sent = match message.method.as_str() {
            JOIN => {
                let reply = if joined.is_some() {
                    JoinReply::Refused {
                        error: "already_joined".into(),
                    }
                } else {
                    match join(&host, &ctx, &channel, &message).await {
                        Ok((snapshot, entered)) => {
                            joined = Some(entered);
                            JoinReply::Joined(snapshot)
                        }
                        Err(reason) => JoinReply::Refused {
                            error: reason.into(),
                        },
                    }
                };
                channel
                    .send_response(message.id, &message.method, &reply)
                    .await
            }
            SET => {
                let reply = match &joined {
                    Some(joined) => set(&joined.door, &message).await,
                    None => SetReply::Refused {
                        error: "not_joined".into(),
                    },
                };
                channel
                    .send_response(message.id, &message.method, &reply)
                    .await
            }
            other => {
                tracing::warn!("知らない request: {other}");
                channel
                    .send_error(
                        message.id,
                        &message.method,
                        &serde_json::json!({ "error": "unknown_request" }),
                    )
                    .await
            }
        };
        if let Err(error) = sent {
            break Err(error);
        }
    };

    if let Some(joined) = joined.take() {
        joined.forwarder.abort();
    }
    result
}

/// 入る: field に入り、feed を開き、変動を書く係を立てる
async fn join(
    host: &Host,
    ctx: &ConnectionContext,
    channel: &Arc<UnisonChannel>,
    message: &ProtocolMessage,
) -> Result<(FieldSnapshot, Joined), &'static str> {
    let request: JoinRequest = message
        .payload_as_value()
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or("bad_request")?;
    let key = FieldKey::new(request.owner, request.name);
    let door = host.find(&key).ok_or("no_such_field")?;
    let (snapshot, visit) = match &door {
        Hosted::Field(field) => {
            let (snapshot, presence) = field.join().await.map_err(|_| "stopped")?;
            (snapshot, Visit::Field(presence))
        }
        Hosted::Relay(relay) => {
            let (snapshot, presence) = relay.join().map_err(|_| "stopped")?;
            (snapshot, Visit::Relay(presence))
        }
    };
    // 応答より先に feed を開く。開けなければ入れない（Presence はここで手放され、出ていく）
    let feed = ctx.open_server_stream(FEED).await.map_err(|error| {
        tracing::warn!("field {key}: feed を開けない: {error}");
        "feed_unavailable"
    })?;
    // 書く係も応答より先に立つので、Update が応答より先に届くことがある。
    // 入った時点の状態は応答で、それより後の変動は feed で届く。
    // 道が違うので届く順番は決まらないが、受け取る側は rev で合流させる
    let forwarder = tokio::spawn(forward(visit, feed, channel.clone()));
    Ok((snapshot, Joined { door, forwarder }))
}

/// 置き換えを頼まれた: field で確定し（中継なら上流へ運び）、確定した変動を返す
async fn set(door: &Hosted, message: &ProtocolMessage) -> SetReply {
    let refused = |error: &str| SetReply::Refused {
        error: error.into(),
    };
    let Some(request) = message
        .payload_as_value()
        .ok()
        .and_then(|value| serde_json::from_value::<SetRequest>(value).ok())
    else {
        return refused("bad_request");
    };
    // 空の要素名は断る。中継なら、上流へ運ぶ前に
    if request.slot.is_empty() {
        return refused("bad_request");
    }
    match door {
        Hosted::Field(field) => set_field(field, request).await,
        Hosted::Relay(relay) => set_relay(relay, request).await,
    }
}

async fn set_field(field: &FieldHandle, request: SetRequest) -> SetReply {
    match field.set(request.slot, request.value).await {
        Ok(update) => SetReply::Set(update),
        Err(FieldError::EmptySlotName) => SetReply::Refused {
            error: "bad_request".into(),
        },
        Err(_) => SetReply::Refused {
            error: "stopped".into(),
        },
    }
}

/// 中継: 上流へ運ぶ。上流が断った理由は、そのまま返す
async fn set_relay(relay: &RelayHandle, request: SetRequest) -> SetReply {
    let error = match relay.set(request.slot, request.value).await {
        Ok(update) => return SetReply::Set(update),
        Err(SyncError::Refused(reason)) => reason,
        Err(SyncError::Closed) => "stopped".into(),
        // 上流に届いたか（確定したか）は分からない。確定していれば feed で届く
        Err(SyncError::Unreachable(detail)) => {
            tracing::warn!("中継 {}: 上流に届かない: {detail}", relay.key());
            "unreachable".into()
        }
    };
    SetReply::Refused { error }
}

/// field の変動を、入っている人の feed へ書き続ける。
///
/// `Visit::next` は前回から変わった要素の今の値だけを返すので、feed への書き込みが
/// 詰まっている間に同じ要素が何度変わっても、書くのは最新の 1 つだけになる。
///
/// field（中継なら、中継か上流）が止まったとき、または feed に書けなくなったとき
/// （相手が feed を受け取っていない、切れた）は、presence も閉じて終わる。presence が閉じる = 出ていく、なので、
/// feed を失ったまま入っている人は残らない
async fn forward(mut visit: Visit, feed: UnisonStream, channel: Arc<UnisonChannel>) {
    'feed: while let Some(updates) = visit.next().await {
        for update in &updates {
            let frame = serde_json::to_value(update).and_then(|payload| {
                ProtocolMessage::new_with_json(0, UPDATE.to_string(), MessageType::Event, payload)
                    .map_err(serde::ser::Error::custom)
            });
            let Ok(frame) = frame else {
                tracing::warn!("Update を組み立てられない: {update:?}");
                continue;
            };
            if let Err(error) = feed.send_frame(&frame).await {
                tracing::debug!("feed に書けない。出ていったことにする: {error}");
                break 'feed;
            }
        }
    }
    // 先に出ていく（入っている人に数えない）。それから、相手に伝わるよう、こちらから閉じる
    drop(visit);
    let _ = feed.close_stream().await;
    let _ = channel.close().await;
}
