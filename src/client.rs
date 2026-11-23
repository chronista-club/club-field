//! field に入って、手元に Slave（field の写し）を持つクライアント。
//!
//! 入ると、手元に [`Slave`] ができる。裏の受け取り係が feed の変動を吸い上げて、
//! Slave を最新に保つ。手元の利用者は通信を意識せず、[`FieldSlave`] を通して
//! Slave を読み（`get` / `snapshot`）、変動を待ち（`changed`）、置き換えを頼む（`set`）。
//!
//! 入るまで（`Join` に field の名札を載せ、名札の付いた状態を受け取る）が field のもの。
//! 入った後の写し（feed の吸い上げ、`Set` の運び方、同時に出す頼みの数の抑え）は
//! [`ConnectedSlave`]（`unison::sync`）に任せる。
//!
//! 結合テストと `field watch` / `field set` / `field relay` が使う。描画などはしない。
//! 証明書を検証しないので、つなげる先は自分のマシンの中（loopback）だけ。

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use unison::ProtocolClient;
use unison::network::{UnisonChannel, UnisonStream};

use crate::core::{FieldInfo, FieldKey, FieldSnapshot};
use crate::protocol::{FEED, JOIN, JoinReply, JoinRequest, PRESENCE};
use crate::sync::{ConnectedSlave, FEED_PATIENCE, FeedAcceptor, Slave};

/// 入れなかった理由。`Refused` の中身は `schemas/field.kdl` の error
pub use crate::sync::JoinError;

/// 入っている field の、手元の写し。
///
/// [`Slave`] として読める（`get` / `snapshot` / `changed` / `set`、中継の `Source`）。
/// 手放す（drop する）と出ていく
pub struct FieldSlave {
    info: FieldInfo,
    slave: ConnectedSlave,
}

impl FieldSlave {
    /// 入る。手元に Slave を立て、裏で feed を吸い上げ始める
    pub async fn join(addr: SocketAddr, key: &FieldKey) -> Result<Self, JoinError> {
        let client = ProtocolClient::insecure_localhost()
            .map_err(|error| JoinError::Network(error.to_string()))?;
        // feed は field の側から開かれる。つなぐ前に受け口を置いておく
        let feed = FeedAcceptor::register(&client, FEED).await;
        // つないだ後に失敗したら、必ず切ってから返す（ProtocolClient は手放しても切れない）
        let (snapshot, channel, feed) = match enter(&client, addr, key, feed).await {
            Ok(entered) => entered,
            Err(error) => {
                let _ = client.disconnect().await;
                return Err(error);
            }
        };
        let info = snapshot.info();
        let slave = ConnectedSlave::attach(snapshot.into_sync(), channel, feed, client);
        Ok(Self { info, slave })
    }

    /// 入った field の名札と代
    pub fn info(&self) -> &FieldInfo {
        &self.info
    }

    /// 出ていく。つながりを閉じ終えるまで待つ
    pub async fn leave(self) {
        self.slave.leave().await;
    }

    /// 名札と写しに分ける（中継として預けるとき）
    pub(crate) fn into_parts(self) -> (FieldInfo, ConnectedSlave) {
        (self.info, self.slave)
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
    feed: FeedAcceptor,
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
    let feed = feed.wait(FEED_PATIENCE).await?;
    Ok((snapshot, channel, feed))
}
