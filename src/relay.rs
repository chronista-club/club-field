//! 中継。上流の field に入っている写し（Slave）を預かり、下流の Slave への配り手にする。
//!
//! 中継そのもの（入る・頼む・止まる）は [`unison::sync::Relay`] に任せる。ここで足すのは、
//! 上流の field の名札（持ち主・名前・場所・代）と、[`Host`](crate::host::Host) で引くための鍵だけ。
//!
//! [`Host`](crate::host::Host) に field（正本）と並べて置くと、protocol が同じ道
//! （presence / feed）で下流を待ち受ける。下流から見ると、field に入るのと変わらない。
//!
//! - 下流が入る: 中継の手元の状態を、上流の field の名札を付けて渡す（[`RelayHandle::join`]）。
//!   中継の手元は、上流より遅れていることがある
//! - 下流が頼む: 中継の Slave の上流への口へ渡す（[`RelayHandle::set_bytes`]）。上流が QUIC の field なら、
//!   頼みは上流の presence（JSON の道）で運ばれる（設計の D18）。上流で確定した変動は、
//!   応答の道で中継の Slave にも合流し、下流へは応答と feed の両方で届く。rev は Origin が振ったまま
//! - 上流が止まる・切れる: 中継の Slave が閉じ、下流の購読が終わる。下流は外れ、以後は入れない
//! - 中継が止まる（[`RelayHandle::stop`]）: 写しを手放して上流から出ていく。下流も同じく外れる
//!
//! 頼みを運ぶ間、lock は握らない（`Relay` の作り）。上流の返事を待っている頼みがあっても、
//! ほかの下流の「入る」「頼む」は待たされない。

use bytes::Bytes;
use serde_json::Value;

use crate::client::FieldSlave;
use crate::core::{FieldError, FieldInfo, FieldKey, FieldSnapshot, json_payload};
use crate::sync::{Relay, SlotUpdate, Subscription, SyncError};

/// 中継を通して入っている、ということ。手放す（drop する）と出ていく
pub use crate::sync::RelayPresence;

/// 預かっている中継への口。複製して配ってよい
#[derive(Clone)]
pub struct RelayHandle {
    key: FieldKey,
    info: FieldInfo,
    relay: Relay,
}

impl RelayHandle {
    /// QUIC で上流の field に入っている写しを預かる
    pub(crate) fn new(slave: FieldSlave) -> Self {
        let (info, slave) = slave.into_parts();
        Self::hold(info, Relay::new(slave))
    }

    /// 中継に、上流の field の名札を付ける
    pub(crate) fn hold(info: FieldInfo, relay: Relay) -> Self {
        debug_assert_eq!(info.life, relay.life());
        Self {
            key: FieldKey::new(info.owner.clone(), info.name.clone()),
            info,
            relay,
        }
    }

    pub fn key(&self) -> &FieldKey {
        &self.key
    }

    /// 中継している field の名札と代
    pub fn info(&self) -> &FieldInfo {
        &self.info
    }

    /// 下流が入る。中継の手元の状態と、入っていること（以後の変動）を渡す。
    /// 止めた・上流から外れた中継には入れない
    pub fn join(&self) -> Result<(FieldSnapshot, RelayPresence), FieldError> {
        let (state, presence) = self.relay.join().map_err(|_| FieldError::Stopped)?;
        Ok((FieldSnapshot::new(self.info.clone(), state), presence))
    }

    /// 下流からの置き換えの頼み（値のバイト列）を、上流へ運ぶ。上流で確定した変動を返し、中継の手元にも合流させる。
    /// 空の要素名は上流へ運ぶ前に断る。上流が断ったら、その理由をそのまま返す。
    ///
    /// 上流が QUIC の field（`field relay`）のときは、頼みは上流の presence（JSON の道）を通るので、
    /// JSON でない値は化けて確定する（設計の D18）。バイト列のまま届くのは、上流が同じプロセスにあるときだけ
    pub async fn set_bytes(
        &self,
        slot: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<SlotUpdate, SyncError> {
        self.relay.set_bytes(slot, payload).await
    }

    /// 下流からの置き換えの頼みを、JSON の値で（[`RelayHandle::set_bytes`] の便利な入口）
    pub async fn set(
        &self,
        slot: impl Into<String>,
        value: Value,
    ) -> Result<SlotUpdate, SyncError> {
        self.set_bytes(slot, json_payload(&value)).await
    }

    /// 中継の手元の状態。止めていれば `None`
    pub fn snapshot(&self) -> Option<FieldSnapshot> {
        let state = self.relay.snapshot()?;
        Some(FieldSnapshot::new(self.info.clone(), state))
    }

    /// 入らずに見る（中継のプロセスの表示など。入っている人には数えない）。
    /// 上流から外れると、購読は残りを返したあとに終わる。止めていれば `None`
    pub fn observe(&self) -> Option<Subscription> {
        self.relay.observe()
    }

    /// 中継を通して入っている人の数
    pub fn visitors(&self) -> usize {
        self.relay.visitors()
    }

    /// 止めた、または上流から外れたか
    pub fn is_stopped(&self) -> bool {
        self.relay.is_stopped()
    }

    /// 止める: 写しを手放して上流から出ていく（つながりを閉じ終えるまで待つ）。
    /// 中継を通して入っていた人は全員外れる。すでに止めていれば何もしない
    pub async fn stop(&self) {
        self.relay.stop().await;
    }
}

#[cfg(test)]
mod tests {
    //! 中継そのものの振る舞い（頼みが上流で確定して合流する、上流の返事を待つ頼みがあっても
    //! ほかの下流は待たされない、止めると返事待ちの頼みも終わる、など）は、
    //! club-unison の `tests/test_small_sync.rs`（`relay_handle_*`）が確かめている。
    //! ここで確かめるのは、field が足したもの（名札と、`FieldError` への読み替え）だけ。

    use std::future::Future;
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::core::Place;
    use crate::sync::{Origin, Slave, SlotState};

    const PATIENCE: Duration = Duration::from_secs(5);

    async fn within<T>(what: &str, work: impl Future<Output = T>) -> T {
        tokio::time::timeout(PATIENCE, work)
            .await
            .unwrap_or_else(|_| panic!("時間切れ: {what}"))
    }

    fn info(life: u64) -> FieldInfo {
        FieldInfo {
            owner: "mako".into(),
            name: "studio".into(),
            place: Place::Cloud,
            life,
        }
    }

    /// 同じプロセスの Origin に入っている Slave を中継として預かる
    fn relay_of(origin: &Origin) -> RelayHandle {
        RelayHandle::hold(
            info(origin.life()),
            Relay::from_slave(Slave::follow(origin)),
        )
    }

    #[tokio::test]
    async fn join_carries_field_name_tag() {
        let origin = Origin::new(3);
        origin.set("light", json!(1)).unwrap();
        let relay = relay_of(&origin);
        assert_eq!(relay.key(), &FieldKey::new("mako", "studio"));
        assert_eq!(relay.info(), &info(3));

        let (snapshot, mut presence) = relay.join().unwrap();
        assert_eq!(snapshot.info(), info(3));
        assert_eq!(
            snapshot.slots.get("light"),
            Some(&SlotState::from_json(1, &json!(1)))
        );
        assert_eq!(relay.snapshot(), Some(snapshot));
        assert_eq!(relay.visitors(), 1);
        // Origin の変動は、中継の Slave を経て届く。rev は Origin が振ったまま
        origin.set("camera", json!("wide")).unwrap();
        assert_eq!(
            within("変動", presence.next()).await,
            Some(vec![SlotUpdate::from_json(3, "camera", 1, &json!("wide"))])
        );
    }

    #[tokio::test]
    async fn set_bytes_carries_payload_upstream_as_is() {
        let origin = Origin::new(1);
        let relay = relay_of(&origin);
        // JSON でないバイト列（protobuf の varint の形）
        let raw = Bytes::from_static(&[0x08, 0x96, 0x01]);
        let confirmed = within("頼む", relay.set_bytes("mesh", raw.clone()))
            .await
            .unwrap();
        assert_eq!(confirmed, SlotUpdate::from_bytes(1, "mesh", 1, raw.clone()));
        // 上流の正本も、渡したバッファそのもの
        let upstream = origin.get("mesh").unwrap();
        assert_eq!((upstream.rev, upstream.payload.as_ptr()), (1, raw.as_ptr()));
    }

    #[tokio::test]
    async fn upstream_stop_refuses_join_as_stopped() {
        let origin = Origin::new(1);
        let relay = relay_of(&origin);
        let (_, mut presence) = relay.join().unwrap();

        origin.close();
        assert_eq!(within("外れる", presence.next()).await, None);
        assert!(relay.is_stopped());
        // 上流から外れた中継には入れない（field に入れないときと同じ理由）
        assert!(matches!(relay.join(), Err(FieldError::Stopped)));
        assert_eq!(relay.set("light", json!(1)).await, Err(SyncError::Closed));
    }

    #[tokio::test]
    async fn stopped_relay_refuses_join_as_stopped() {
        let origin = Origin::new(1);
        let relay = relay_of(&origin);
        let (_, mut presence) = relay.join().unwrap();

        relay.stop().await;
        assert_eq!(within("外れる", presence.next()).await, None);
        assert!(matches!(relay.join(), Err(FieldError::Stopped)));
        assert!(relay.snapshot().is_none());
        assert!(relay.observe().is_none());
        // Origin は動き続ける
        assert!(origin.set("light", json!(1)).is_ok());
    }
}
