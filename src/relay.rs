//! 中継。上流の field に入っている写し（Slave）を預かり、下流の Slave への配り手にする。
//!
//! [`Host`](crate::host::Host) に field（正本）と並べて置くと、protocol が同じ道
//! （presence / feed）で下流を待ち受ける。下流から見ると、field に入るのと変わらない。
//!
//! - 下流が入る: 中継の手元の状態と、中継の Slave の購読を渡す（[`RelayHandle::join`]）。
//!   中継の手元は、上流より遅れていることがある
//! - 下流が頼む: 中継の Slave の上流への口へ渡す（[`RelayHandle::set`]）。上流で確定した変動は、
//!   応答の道で中継の Slave にも合流し、下流へは応答と feed の両方で届く。rev は Origin が振ったまま
//! - 上流が止まる・切れる: 中継の Slave が閉じ、下流の購読が終わる。下流は外れ、以後は入れない
//! - 中継が止まる（[`RelayHandle::stop`]）: 写しを手放して上流から出ていく。下流も同じく外れる
//!
//! 頼みを運ぶ間、lock は握らない。上流の返事を待っている頼みがあっても、
//! ほかの下流の「入る」「頼む」は待たされない。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;
use tokio::task::JoinHandle;

use crate::client::FieldSlave;
use crate::core::{FieldError, FieldInfo, FieldKey, FieldSnapshot};
use crate::sync::{Slave, SlotUpdate, Source, Subscription, SyncError};

/// 預かっている中継への口。複製して配ってよい
#[derive(Clone)]
pub struct RelayHandle {
    inner: Arc<Inner>,
}

struct Inner {
    key: FieldKey,
    info: FieldInfo,
    /// 預かっている写し。止めると `None`
    held: Mutex<Option<Held>>,
    /// 中継を通して入っている人の数
    visitors: Arc<AtomicUsize>,
}

struct Held {
    slave: Slave,
    /// 写しを手放したあと、上流とのつながりを閉じ終える係（QUIC の吸い上げる係）
    pump: Option<JoinHandle<()>>,
}

/// 中継を通して入っている、ということ。手放す（drop する）と出ていく
pub struct RelayPresence {
    /// 入った時点より後の、中継の Slave の変動
    updates: Subscription,
    _visit: Visit,
}

impl RelayPresence {
    /// 次の変動を待つ。前回から変わった要素の今の値を、変わった順に返す（途中の rev は畳まれる）。
    /// 中継が止まる・上流から外れると `None`
    pub async fn next(&mut self) -> Option<Vec<SlotUpdate>> {
        self.updates.next().await
    }
}

/// 入っている人の数に 1 つ数える印
struct Visit(Arc<AtomicUsize>);

impl Drop for Visit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl RelayHandle {
    /// QUIC で上流に入っている写しを預かる
    pub(crate) fn new(slave: FieldSlave) -> Self {
        let (info, slave, pump) = slave.into_parts();
        Self::hold(info, slave, Some(pump))
    }

    /// 写しを預かる。`pump` は、写しを手放したあとに終わるのを待つ係
    pub(crate) fn hold(info: FieldInfo, slave: Slave, pump: Option<JoinHandle<()>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                key: FieldKey::new(info.owner.clone(), info.name.clone()),
                info,
                held: Mutex::new(Some(Held { slave, pump })),
                visitors: Arc::new(AtomicUsize::new(0)),
            }),
        }
    }

    pub fn key(&self) -> &FieldKey {
        &self.inner.key
    }

    /// 中継している field の名札と代
    pub fn info(&self) -> &FieldInfo {
        &self.inner.info
    }

    /// 下流が入る。中継の手元の状態と、入っていること（以後の変動）を渡す。
    /// 止めた・上流から外れた中継には入れない
    pub fn join(&self) -> Result<(FieldSnapshot, RelayPresence), FieldError> {
        let held = self.inner.lock();
        let slave = held.as_ref().ok_or(FieldError::Stopped)?;
        if slave.slave.is_closed() {
            return Err(FieldError::Stopped);
        }
        let (state, updates) = Source::subscribe(&slave.slave);
        self.inner.visitors.fetch_add(1, Ordering::Relaxed);
        let presence = RelayPresence {
            updates,
            _visit: Visit(self.inner.visitors.clone()),
        };
        Ok((FieldSnapshot::new(self.inner.info.clone(), state), presence))
    }

    /// 下流からの置き換えの頼みを、上流へ運ぶ。上流で確定した変動を返し、中継の手元にも合流させる。
    /// 上流が断ったら、その理由をそのまま返す
    pub async fn set(
        &self,
        slot: impl Into<String>,
        value: Value,
    ) -> Result<SlotUpdate, SyncError> {
        // 上流への口だけを取り出し、lock を手放してから待つ
        let upstream = {
            let held = self.inner.lock();
            match held.as_ref() {
                Some(held) if !held.slave.is_closed() => Source::upstream(&held.slave),
                _ => return Err(SyncError::Closed),
            }
        };
        upstream.set(slot.into(), value).await
    }

    /// 中継の手元の状態。止めていれば `None`
    pub fn snapshot(&self) -> Option<FieldSnapshot> {
        let held = self.inner.lock();
        let held = held.as_ref()?;
        Some(FieldSnapshot::new(
            self.inner.info.clone(),
            held.slave.snapshot(),
        ))
    }

    /// 入らずに見る（中継のプロセスの表示など。入っている人には数えない）。
    /// 上流から外れると、購読は残りを返したあとに終わる。止めていれば `None`
    pub fn observe(&self) -> Option<Subscription> {
        let held = self.inner.lock();
        Some(Source::subscribe(&held.as_ref()?.slave).1)
    }

    /// 中継を通して入っている人の数
    pub fn visitors(&self) -> usize {
        self.inner.visitors.load(Ordering::Relaxed)
    }

    /// 止めた、または上流から外れたか
    pub fn is_stopped(&self) -> bool {
        self.inner
            .lock()
            .as_ref()
            .is_none_or(|held| held.slave.is_closed())
    }

    /// 止める: 写しを手放して上流から出ていく（つながりを閉じ終えるまで待つ）。
    /// 中継を通して入っていた人は全員外れる。すでに止めていれば何もしない
    pub async fn stop(&self) {
        let held = self.inner.lock().take();
        if let Some(Held { slave, pump }) = held {
            drop(slave);
            if let Some(pump) = pump {
                let _ = pump.await;
            }
        }
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, Option<Held>> {
        self.held.lock().expect("中継の lock が壊れている")
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::core::Place;
    use crate::sync::{Origin, SetFuture, SlotState, Snapshot, Upstream};

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
            place: Place::Local,
            life,
        }
    }

    fn state(rev: u64, value: Value) -> SlotState {
        SlotState { rev, value }
    }

    /// 同じプロセスの Origin に入っている Slave を中継として預かる
    fn relay_of(origin: &Origin) -> RelayHandle {
        RelayHandle::hold(info(origin.life()), Slave::follow(origin), None)
    }

    /// `slow` という要素の頼みにだけ、開けるまで返事をしない上流
    struct Gate {
        open: Semaphore,
        rev: AtomicU64,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                open: Semaphore::new(0),
                rev: AtomicU64::new(0),
            })
        }
    }

    impl Upstream for Gate {
        fn set(&self, slot: String, value: Value) -> SetFuture<'_> {
            Box::pin(async move {
                if slot == "slow" {
                    let _ = self.open.acquire().await;
                }
                Ok(SlotUpdate {
                    life: 1,
                    slot,
                    rev: self.rev.fetch_add(1, Ordering::Relaxed) + 1,
                    value,
                })
            })
        }
    }

    /// Gate を上流に持つ中継。写しを手放すと上流から外れる（吸い上げる係の代わりを立てる）
    fn relay_behind(gate: Arc<Gate>) -> RelayHandle {
        let snapshot = Snapshot {
            life: 1,
            ..Snapshot::default()
        };
        let (slave, inlet) = Slave::new(snapshot, gate);
        let pump = tokio::spawn(async move { inlet.slave_gone().await });
        RelayHandle::hold(info(1), slave, Some(pump))
    }

    #[tokio::test]
    async fn join_gets_relay_state_then_updates() {
        let origin = Origin::new(1);
        origin.set("light", json!(1)).unwrap();
        let relay = relay_of(&origin);
        assert_eq!(relay.key(), &FieldKey::new("mako", "studio"));

        let (snapshot, mut presence) = relay.join().unwrap();
        assert_eq!((snapshot.owner.as_str(), snapshot.life), ("mako", 1));
        assert_eq!(snapshot.slots.get("light"), Some(&state(1, json!(1))));
        // Origin の変動は、中継の Slave を経て届く。rev は Origin が振ったまま
        origin.set("camera", json!("wide")).unwrap();
        assert_eq!(
            within("変動", presence.next()).await,
            Some(vec![SlotUpdate {
                life: 1,
                slot: "camera".into(),
                rev: 1,
                value: json!("wide"),
            }])
        );
    }

    #[tokio::test]
    async fn set_is_confirmed_upstream_and_merged_into_relay() {
        let origin = Origin::new(1);
        let relay = relay_of(&origin);
        let (_, mut presence) = relay.join().unwrap();

        let confirmed = within("頼む", relay.set("light", json!(2))).await.unwrap();
        assert_eq!((confirmed.rev, &confirmed.value), (1, &json!(2)));
        assert_eq!(origin.get("light"), Some(state(1, json!(2))));
        // 応答の道で、中継の手元にも合流している（知らせを待たない）
        assert_eq!(
            relay.snapshot().unwrap().slots.get("light"),
            Some(&state(1, json!(2)))
        );
        // 中継を通して入っている人には feed でも届く
        assert_eq!(within("変動", presence.next()).await, Some(vec![confirmed]));
    }

    #[tokio::test]
    async fn set_waiting_upstream_blocks_nobody() {
        let gate = Gate::new();
        let relay = relay_behind(gate.clone());
        let slow = tokio::spawn({
            let relay = relay.clone();
            async move { relay.set("slow", json!(1)).await }
        });
        tokio::task::yield_now().await;

        // 上流の返事を待っている頼みがあっても、ほかの下流は入れて、頼める
        let (_, _presence) = relay.join().unwrap();
        let fast = within("ほかの頼み", relay.set("fast", json!(2)))
            .await
            .unwrap();
        assert_eq!(fast.slot, "fast");
        assert!(!slow.is_finished());

        gate.open.add_permits(1);
        let slow = within("待っていた頼み", slow).await.unwrap().unwrap();
        assert_eq!(slow.slot, "slow");
        assert_eq!(relay.snapshot().unwrap().slots.len(), 2);
    }

    #[tokio::test]
    async fn leaving_visitor_is_not_counted() {
        let origin = Origin::new(1);
        let relay = relay_of(&origin);
        let (_, a) = relay.join().unwrap();
        let (_, _b) = relay.join().unwrap();
        assert_eq!(relay.visitors(), 2);
        drop(a);
        assert_eq!(relay.visitors(), 1);
        // 中継は動き続ける
        assert!(relay.set("light", json!(1)).await.is_ok());
    }

    #[tokio::test]
    async fn upstream_stop_drops_visitors() {
        let origin = Origin::new(1);
        let relay = relay_of(&origin);
        let (_, mut presence) = relay.join().unwrap();
        let mut watching = relay.observe().unwrap();

        origin.close();
        // 中継を通して入っていた人には「止まった」が伝わる
        assert_eq!(within("外れる", presence.next()).await, None);
        assert_eq!(within("外れる", watching.next()).await, None);
        assert!(relay.is_stopped());
        // 上流から外れた中継には入れず、頼めない
        assert!(matches!(relay.join(), Err(FieldError::Stopped)));
        assert_eq!(relay.set("light", json!(1)).await, Err(SyncError::Closed));
    }

    #[tokio::test]
    async fn stop_leaves_upstream_and_drops_visitors() {
        let origin = Origin::new(1);
        let relay = relay_of(&origin);
        let (_, mut presence) = relay.join().unwrap();

        relay.stop().await;
        assert_eq!(within("外れる", presence.next()).await, None);
        assert!(relay.is_stopped());
        assert!(matches!(relay.join(), Err(FieldError::Stopped)));
        assert_eq!(relay.set("light", json!(1)).await, Err(SyncError::Closed));
        assert!(relay.snapshot().is_none());
        assert!(relay.observe().is_none());
        // 2 度めは何もしない
        relay.stop().await;
        // Origin は動き続ける
        assert!(origin.set("light", json!(1)).is_ok());
    }

    #[tokio::test]
    async fn stop_ends_pending_set() {
        let relay = relay_behind(Gate::new());
        let slow = tokio::spawn({
            let relay = relay.clone();
            async move { relay.set("slow", json!(1)).await }
        });
        tokio::task::yield_now().await;
        // 中継が止まると、上流の返事を待っていた頼みも終わる
        within("止める", relay.stop()).await;
        assert_eq!(
            within("頼みが終わる", slow).await.unwrap(),
            Err(SyncError::Closed)
        );
    }
}
