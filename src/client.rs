//! field に入って、手元に Slave（field の写し）を持つクライアント。
//!
//! 入ると、手元に [`Slave`] ができる。裏の受け取り係が feed の変動を吸い上げて、
//! Slave を最新に保つ。手元の利用者は通信を意識せず、[`FieldSlave`] を通して
//! Slave を読み（`get` / `snapshot`）、変動を待ち（`changed`）、置き換えを頼む（`set`）。
//!
//! 結合テストと `field watch` / `field set` が使う。描画などはしない。
//! 証明書を検証しないので、つなげる先は自分のマシンの中（loopback）だけ。

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Semaphore, oneshot};
use unison::ProtocolClient;
use unison::network::{MessageType, UnisonChannel, UnisonStream};

use crate::core::{FieldInfo, FieldKey, FieldSnapshot};
use crate::protocol::{
    FEED, JOIN, JoinReply, JoinRequest, PRESENCE, SET, SetReply, SetRequest, UPDATE,
};
use crate::sync::{Inlet, SetFuture, Slave, SlotUpdate, SyncError, Upstream};

/// 入った後、field の側から feed が開かれるまで待つ時間
const FEED_PATIENCE: Duration = Duration::from_secs(5);

/// 1 つの Slave から同時に出せる、置き換えの頼みの数。
///
/// field の側は、presence に届いた request を待ち行列（club-unison の `UnisonChannel`、256 件）
/// から 1 つずつ処理する。待ち行列が満ちると、届いた request は黙って捨てられ、頼んだ側は
/// 応答を待ったまま時間切れになる。同時に出す数をそれより小さく抑え、超えたぶんは手元で待たせる
const MAX_SETS_IN_FLIGHT: usize = 128;

#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    /// field の側が断った。中身は理由（`schemas/field.kdl` の error）
    #[error("入れなかった: {0}")]
    Refused(String),
    #[error("つながらなかった: {0}")]
    Network(String),
}

/// 入っている field の、手元の写し。
///
/// [`Slave`] として読める（`get` / `snapshot` / `changed` / `set`、中継の `Source`）。
/// 手放す（drop する）と出ていく
pub struct FieldSlave {
    info: FieldInfo,
    slave: Slave,
    /// feed を吸い上げる係
    pump: tokio::task::JoinHandle<()>,
}

impl FieldSlave {
    /// 入る。手元に Slave を立て、裏で feed を吸い上げ始める
    pub async fn join(addr: SocketAddr, key: &FieldKey) -> Result<Self, JoinError> {
        let network = |error: &dyn std::fmt::Display| JoinError::Network(error.to_string());

        let client = ProtocolClient::insecure_localhost().map_err(|e| network(&e))?;
        // feed は field の側から開かれる。つなぐ前に受け口を置いておく
        let (feed_tx, feed_rx) = oneshot::channel::<UnisonStream>();
        let feed_tx = Arc::new(Mutex::new(Some(feed_tx)));
        client
            .register_server_channel(FEED, move |stream| {
                let feed_tx = feed_tx.clone();
                async move {
                    let sender = feed_tx.lock().expect("feed の受け口が壊れている").take();
                    match sender {
                        Some(sender) => {
                            let _ = sender.send(stream);
                        }
                        None => tracing::warn!("feed が 2 本目に開かれた。捨てる"),
                    }
                    Ok(())
                }
            })
            .await;
        // つないだ後に失敗したら、必ず切ってから返す（ProtocolClient は手放しても切れない）
        let (snapshot, channel, feed) = match enter(&client, addr, key, feed_rx).await {
            Ok(entered) => entered,
            Err(error) => {
                let _ = client.disconnect().await;
                return Err(error);
            }
        };

        let info = snapshot.info();
        let upstream = Arc::new(QuicUpstream {
            channel: channel.clone(),
            in_flight: Semaphore::new(MAX_SETS_IN_FLIGHT),
        });
        let (slave, inlet) = Slave::new(snapshot.into_sync(), upstream);
        let pump = tokio::spawn(pump(feed, inlet, channel, client));
        Ok(Self { info, slave, pump })
    }

    /// 入った field の名札と代
    pub fn info(&self) -> &FieldInfo {
        &self.info
    }

    /// 出ていく。つながりを閉じ終えるまで待つ
    pub async fn leave(self) {
        let Self { slave, pump, .. } = self;
        drop(slave);
        let _ = pump.await;
    }

    /// 名札・写し・吸い上げる係に分ける（中継として預けるとき）。
    /// 写しを手放すと出ていき、吸い上げる係はつながりを閉じ終えたら終わる
    pub(crate) fn into_parts(self) -> (FieldInfo, Slave, tokio::task::JoinHandle<()>) {
        (self.info, self.slave, self.pump)
    }
}

impl Deref for FieldSlave {
    type Target = Slave;

    fn deref(&self) -> &Slave {
        &self.slave
    }
}

impl DerefMut for FieldSlave {
    fn deref_mut(&mut self) -> &mut Slave {
        &mut self.slave
    }
}

/// つなぎ、presence を開き、入り、feed が開かれるのを待つ
async fn enter(
    client: &ProtocolClient,
    addr: SocketAddr,
    key: &FieldKey,
    feed_rx: oneshot::Receiver<UnisonStream>,
) -> Result<(FieldSnapshot, Arc<UnisonChannel>, UnisonStream), JoinError> {
    let network = |error: &dyn std::fmt::Display| JoinError::Network(error.to_string());
    client
        .connect(&addr.to_string())
        .await
        .map_err(|e| network(&e))?;
    let channel = Arc::new(
        client
            .open_channel(PRESENCE)
            .await
            .map_err(|e| network(&e))?,
    );
    let request = JoinRequest {
        owner: key.owner.clone(),
        name: key.name.clone(),
    };
    let reply: JoinReply = channel
        .request(JOIN, &request)
        .await
        .map_err(|e| network(&e))?;
    let snapshot = match reply {
        JoinReply::Joined(snapshot) => snapshot,
        JoinReply::Refused { error } => return Err(JoinError::Refused(error)),
    };
    let feed = match tokio::time::timeout(FEED_PATIENCE, feed_rx).await {
        Ok(Ok(feed)) => feed,
        _ => return Err(JoinError::Network("feed が開かれない".into())),
    };
    Ok((snapshot, channel, feed))
}

/// 置き換えの頼みを、presence の `Set` で field へ運ぶ
struct QuicUpstream {
    channel: Arc<UnisonChannel>,
    /// 同時に出している頼みの数を抑える（[`MAX_SETS_IN_FLIGHT`]）
    in_flight: Semaphore,
}

impl Upstream for QuicUpstream {
    fn set(&self, slot: String, value: Value) -> SetFuture<'_> {
        Box::pin(async move {
            let _permit = self
                .in_flight
                .acquire()
                .await
                .map_err(|_| SyncError::Closed)?;
            let reply: SetReply = self
                .channel
                .request(SET, &SetRequest { slot, value })
                .await
                .map_err(|error| SyncError::Unreachable(error.to_string()))?;
            match reply {
                SetReply::Set(update) => Ok(update),
                SetReply::Refused { error } => Err(SyncError::Refused(error)),
            }
        })
    }
}

/// feed を吸い上げて Slave に流し込む係。
///
/// field が止まる・切れると、Slave に「外れた」を伝えて終わる。
/// Slave が手放されると、つながりを閉じて終わる（= 出ていく）
async fn pump(
    feed: UnisonStream,
    inlet: Inlet,
    channel: Arc<UnisonChannel>,
    client: ProtocolClient,
) {
    loop {
        tokio::select! {
            frame = feed.recv_frame() => match frame {
                Ok(message) if message.msg_type == MessageType::Event && message.method == UPDATE => {
                    let update = message
                        .payload_as_value()
                        .ok()
                        .and_then(|value| serde_json::from_value::<SlotUpdate>(value).ok());
                    match update {
                        Some(update) => {
                            inlet.apply(&update);
                        }
                        None => tracing::warn!("読めない Update を捨てた"),
                    }
                }
                Ok(_) => continue,
                // field が止まった、または切れた
                Err(_) => break,
            },
            _ = inlet.slave_gone() => break,
        }
    }
    inlet.close();
    let _ = channel.close().await;
    let _ = client.disconnect().await;
}
