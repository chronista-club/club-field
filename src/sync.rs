//! Origin ↔ Slave の同期。field を知らない、汎用の部品。
//!
//! 仕様: `docs/spec/01-field.md`（同期のしくみ）／
//! モデル: `docs/model/field.qnt`（実装が選んだ道のりは `streamLatestMerge`）
//!
//! - 状態は、名前の付いた要素（slot）の集まり。要素ごとに `(rev, value)` を持つ
//! - 正本は [`Origin`] の 1 つだけ。[`Origin::set`] が、その要素の rev を 1 つ進めて確定する
//! - 手元の写しが [`Slave`]。届いた変動で、自分の要素を書き換える（自動で同期する）。
//!   手元の利用者は通信を意識せず、[`Slave::get`] / [`Slave::snapshot`] で今の値を読み、
//!   [`Slave::changed`] で変動を待ち、[`Slave::set`] で Origin に置き換えを頼む
//! - Slave の合流の決まり: **同じ代で、手元より新しい rev だけを受け入れる**。
//!   rev は要素ごとに独立しているので、別々の道（stream、応答）から順不同で届いても矛盾しない
//! - Slave も Origin と同じ形の状態を持つので、そのまま下流の Slave への配り手（中継）になれる
//!   （[`Source`]）。rev は Origin が振ったものをそのまま運ぶ
//! - 配り手は、購読者ごとに「前回から変わった要素」だけを、今の値で渡す（途中の rev は畳む）。
//!   受け取りが遅い購読者がいても、溜まるのは要素の数まで
//!
//! ネットワークは持たない。運び方は使う側（field では `protocol` / `client`）が決める。
//! 将来 club-unison 側へそのまま移せるよう、field の語彙（持ち主・名前・場所）を使わない。

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;

/// 要素 1 つの状態
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SlotState {
    /// 世代番号。Origin が確定するたびに 1 つ進む。最初の確定が 1
    pub rev: u64,
    pub value: Value,
}

/// 要素 1 つの変動。Origin が振った rev をそのまま運ぶ
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SlotUpdate {
    /// 何代めの Origin の変動か
    pub life: u64,
    pub slot: String,
    pub rev: u64,
    pub value: Value,
}

/// ある時点の状態を丸ごと
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// 何代めの Origin の状態か
    pub life: u64,
    pub slots: BTreeMap<String, SlotState>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SyncError {
    /// 配り手が止まった、または上流から外れた
    #[error("閉じている")]
    Closed,
    /// 上流が断った。中身は理由
    #[error("断られた: {0}")]
    Refused(String),
    /// 上流に届かなかった
    #[error("上流に届かない: {0}")]
    Unreachable(String),
}

// ── 状態（IO も同期の道具も持たない）──────────────────────

/// 要素の集まり。Origin と Slave が同じものを持ち、違うのは書き換え方
/// （[`Slots::set`] で確定する / [`Slots::merge`] で合流する）だけ
#[derive(Clone, Debug)]
pub struct Slots {
    life: u64,
    entries: BTreeMap<String, Entry>,
    /// 変わった順の索引（この Slots での通し番号 → 要素名）。
    /// 各要素は、最後に変わった 1 か所にだけいる
    order: BTreeMap<u64, String>,
    /// この Slots での通し番号。何かを受け入れるたびに 1 つ進む（rev とは別物）
    seq: u64,
}

#[derive(Clone, Debug)]
struct Entry {
    state: SlotState,
    seq: u64,
}

impl Slots {
    /// まだ何も置かれていない状態
    pub fn new(life: u64) -> Self {
        Self {
            life,
            entries: BTreeMap::new(),
            order: BTreeMap::new(),
            seq: 0,
        }
    }

    /// 丸ごとの状態から作る（Slave の始まり）
    pub fn from_snapshot(snapshot: Snapshot) -> Self {
        let mut slots = Self::new(snapshot.life);
        for (name, state) in snapshot.slots {
            slots.put(name, state);
        }
        slots
    }

    pub fn life(&self) -> u64 {
        self.life
    }

    /// この Slots での通し番号（[`Slots::changes_since`] に渡す）
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn get(&self, slot: &str) -> Option<&SlotState> {
        self.entries.get(slot).map(|entry| &entry.state)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            life: self.life,
            slots: self
                .entries
                .iter()
                .map(|(name, entry)| (name.clone(), entry.state.clone()))
                .collect(),
        }
    }

    /// 置き換える（Origin）: その要素の rev を 1 つ進めて確定する
    pub fn set(&mut self, slot: &str, value: Value) -> SlotUpdate {
        let rev = self.get(slot).map_or(0, |state| state.rev) + 1;
        self.put(
            slot.to_string(),
            SlotState {
                rev,
                value: value.clone(),
            },
        );
        SlotUpdate {
            life: self.life,
            slot: slot.to_string(),
            rev,
            value,
        }
    }

    /// 合流する（Slave）: 同じ代で、手元より新しい rev だけを受け入れる。受け入れたら `true`。
    /// 古い rev、同じ rev、別の代の変動は、何も変えない
    pub fn merge(&mut self, update: &SlotUpdate) -> bool {
        if update.life != self.life || update.rev == 0 {
            return false;
        }
        if self
            .get(&update.slot)
            .is_some_and(|state| state.rev >= update.rev)
        {
            return false;
        }
        self.put(
            update.slot.clone(),
            SlotState {
                rev: update.rev,
                value: update.value.clone(),
            },
        );
        true
    }

    /// 通し番号 `since` より後に変わった要素の、今の値（変わった順）。途中の rev は畳まれている
    pub fn changes_since(&self, since: u64) -> Vec<SlotUpdate> {
        self.order
            .range((Bound::Excluded(since), Bound::Unbounded))
            .map(|(_, name)| {
                let state = &self.entries[name].state;
                SlotUpdate {
                    life: self.life,
                    slot: name.clone(),
                    rev: state.rev,
                    value: state.value.clone(),
                }
            })
            .collect()
    }

    fn put(&mut self, name: String, state: SlotState) {
        self.seq += 1;
        let seq = self.seq;
        if let Some(old) = self.entries.insert(name.clone(), Entry { state, seq }) {
            self.order.remove(&old.seq);
        }
        self.order.insert(seq, name);
    }
}

// ── 共有（Origin と Slave の中身）────────────────────────

/// 購読者への知らせ。中身は「どこまで変わったか」と「閉じたか」だけで、
/// 変わった要素そのものは、購読者が [`Slots`] から引く（だから溜まらない）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Version {
    seq: u64,
    closed: bool,
}

struct Shared {
    slots: Mutex<Slots>,
    version: watch::Sender<Version>,
    /// 閉じたことだけの知らせ。返事を待つ頼み（[`Link`]）が使う。変動では起きない
    closed: watch::Sender<bool>,
}

impl Shared {
    fn new(slots: Slots) -> Arc<Self> {
        let (version, _) = watch::channel(Version {
            seq: slots.seq(),
            closed: false,
        });
        let (closed, _) = watch::channel(false);
        Arc::new(Self {
            slots: Mutex::new(slots),
            version,
            closed,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Slots> {
        self.slots.lock().expect("slots の lock が壊れている")
    }

    /// 書き換えたことを購読者に知らせる。lock を握ったまま呼ぶので、
    /// 購読者が見る通し番号と中身は食い違わない
    fn publish(&self, slots: &Slots) {
        let seq = slots.seq();
        self.version.send_if_modified(|version| {
            let moved = version.seq != seq;
            version.seq = seq;
            moved
        });
    }

    fn close(&self) {
        let _slots = self.lock();
        self.version.send_if_modified(|version| {
            let was = version.closed;
            version.closed = true;
            !was
        });
        self.closed
            .send_if_modified(|closed| !std::mem::replace(closed, true));
    }

    fn is_closed(&self) -> bool {
        self.version.borrow().closed
    }

    /// 購読する。返すのは、購読した時点の状態と、それより後の変動の購読。
    /// 状態は購読の中に持たせない（入った人の数だけ写しを抱えないため）
    fn subscribe(self: &Arc<Self>) -> (Snapshot, Subscription) {
        let slots = self.lock();
        // 今の知らせは「見たもの」として扱われる。以後に届くのは、この snapshot より後の変動だけ
        let version = self.version.subscribe();
        let subscription = Subscription {
            shared: self.clone(),
            version,
            cursor: slots.seq(),
        };
        (slots.snapshot(), subscription)
    }

    fn merge(&self, update: &SlotUpdate) -> bool {
        let mut slots = self.lock();
        let accepted = slots.merge(update);
        if accepted {
            self.publish(&slots);
        }
        accepted
    }
}

/// 配り手の変動の購読。購読した時点より後に変わった要素を受け取る
/// （購読した時点の状態は、購読するときに別に受け取る）
pub struct Subscription {
    shared: Arc<Shared>,
    version: watch::Receiver<Version>,
    /// どこまで渡したか（配り手の [`Slots`] での通し番号）
    cursor: u64,
}

impl Subscription {
    /// 次の変動を待つ。前回から変わった要素の、今の値を変わった順に返す（途中の rev は畳まれる）。
    /// 配り手が閉じたら、残りの変動を返したあとに `None`
    pub async fn next(&mut self) -> Option<Vec<SlotUpdate>> {
        loop {
            let (changes, closed) = {
                let slots = self.shared.lock();
                let changes = slots.changes_since(self.cursor);
                self.cursor = slots.seq();
                (changes, self.version.borrow_and_update().closed)
            };
            if !changes.is_empty() {
                return Some(changes);
            }
            if closed {
                return None;
            }
            self.version.changed().await.ok()?;
        }
    }
}

// ── Origin ─────────────────────────────────────────────

/// 正本。複製して配ってよい（中身は共有）
#[derive(Clone)]
pub struct Origin {
    shared: Arc<Shared>,
}

impl Origin {
    /// `life` 代めの Origin を、要素が空の状態で作る
    pub fn new(life: u64) -> Self {
        Self {
            shared: Shared::new(Slots::new(life)),
        }
    }

    pub fn life(&self) -> u64 {
        self.shared.lock().life()
    }

    pub fn get(&self, slot: &str) -> Option<SlotState> {
        self.shared.lock().get(slot).cloned()
    }

    pub fn snapshot(&self) -> Snapshot {
        self.shared.lock().snapshot()
    }

    /// 置き換える: その要素の rev を 1 つ進めて確定し、購読者に知らせる
    pub fn set(&self, slot: &str, value: Value) -> Result<SlotUpdate, SyncError> {
        let mut slots = self.shared.lock();
        if self.shared.is_closed() {
            return Err(SyncError::Closed);
        }
        let update = slots.set(slot, value);
        self.shared.publish(&slots);
        Ok(update)
    }

    /// 変動を購読する。購読した時点の状態と、以後の変動の購読を返す
    pub fn subscribe(&self) -> (Snapshot, Subscription) {
        self.shared.subscribe()
    }

    /// 閉じる（止まる）。購読者は、残りの変動を受け取ったあとに終わる。以後は置き換えられない
    pub fn close(&self) {
        self.shared.close();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }
}

// ── 上流と配り手 ─────────────────────────────────────────

/// 置き換えの頼みの返事を待つ future
pub type SetFuture<'a> = Pin<Box<dyn Future<Output = Result<SlotUpdate, SyncError>> + Send + 'a>>;

/// 上流（Origin の側）へ置き換えを頼む口。運び方ごとに実装する
pub trait Upstream: Send + Sync {
    /// 置き換えを頼み、Origin が確定した変動を返す
    fn set(&self, slot: String, value: Value) -> SetFuture<'_>;
}

impl Upstream for Origin {
    fn set(&self, slot: String, value: Value) -> SetFuture<'_> {
        Box::pin(std::future::ready(Origin::set(self, &slot, value)))
    }
}

/// 配り手。Origin も、Slave（中継）もなれる
pub trait Source {
    /// 変動を購読する。購読した時点の状態と、以後の変動の購読を返す
    fn subscribe(&self) -> (Snapshot, Subscription);
    /// 置き換えの頼みの行き先
    fn upstream(&self) -> Arc<dyn Upstream>;
}

impl Source for Origin {
    fn subscribe(&self) -> (Snapshot, Subscription) {
        Origin::subscribe(self)
    }

    fn upstream(&self) -> Arc<dyn Upstream> {
        Arc::new(self.clone())
    }
}

// ── Slave ──────────────────────────────────────────────

/// 手元の写し。届いた変動で、自分の要素を書き換える。
///
/// 手放す（drop する）と上流から外れる: 吸い上げる係が止まり、
/// この Slave を通して入っている下流の購読も終わる
pub struct Slave {
    link: Arc<Link>,
    /// 手元の利用者のための購読（[`Slave::changed`]）
    watch: Subscription,
    /// 生きている印。これが手放されると、[`Inlet::slave_gone`] が返る
    _alive: watch::Receiver<()>,
}

/// Slave の中身と、上流への口。中継では、下流からの頼みの行き先にもなる
struct Link {
    shared: Arc<Shared>,
    upstream: Arc<dyn Upstream>,
}

impl Upstream for Link {
    fn set(&self, slot: String, value: Value) -> SetFuture<'_> {
        Box::pin(async move {
            if self.shared.is_closed() {
                return Err(SyncError::Closed);
            }
            // 返事を待つ間に上流から外れたら、待つのをやめる。
            // 外れた上流の返事は、届かないか、届いても遅れて（運び方の時間切れで）しか来ない。
            // 待つのは「閉じた」だけの知らせ。変動の知らせ（version）だと、変動のたびに起きてしまう
            let mut closed = self.shared.closed.subscribe();
            let confirmed = tokio::select! {
                biased;
                confirmed = self.upstream.set(slot, value) => confirmed?,
                _ = closed.wait_for(|closed| *closed) => return Err(SyncError::Closed),
            };
            // 応答の道でも合流させる。頼んだ側は、変動の知らせを待たずに確定した値を持つ。
            // 知らせのほうが先に届いていても、古い rev は捨てられるので戻らない
            self.shared.merge(&confirmed);
            Ok(confirmed)
        })
    }
}

/// 上流から届いた変動を Slave に流し込む口。運び方の受け取り係が持つ。
/// 手放すと、Slave は上流から外れたことになる（[`Inlet::close`] と同じ）
pub struct Inlet {
    link: Arc<Link>,
    alive: watch::Sender<()>,
}

impl Inlet {
    /// 届いた変動を合流させる。受け入れたら `true`（古い rev・別の代は捨てる）
    pub fn apply(&self, update: &SlotUpdate) -> bool {
        self.link.shared.merge(update)
    }

    /// 上流が切れた・止まった。Slave と、その下流の購読者に伝わる
    pub fn close(&self) {
        self.link.shared.close();
    }

    /// Slave が手放されるまで待つ（受け取り係が、自分の後始末に使う）
    pub async fn slave_gone(&self) {
        self.alive.closed().await;
    }
}

impl Drop for Inlet {
    fn drop(&mut self) {
        self.close();
    }
}

impl Slave {
    /// 初めの状態と、上流への口から Slave を作る。
    /// 返す [`Inlet`] に、上流から届いた変動を流し込む
    pub fn new(snapshot: Snapshot, upstream: Arc<dyn Upstream>) -> (Slave, Inlet) {
        let shared = Shared::new(Slots::from_snapshot(snapshot));
        let link = Arc::new(Link {
            shared: shared.clone(),
            upstream,
        });
        let (alive, alive_rx) = watch::channel(());
        let slave = Slave {
            link: link.clone(),
            watch: shared.subscribe().1,
            _alive: alive_rx,
        };
        (slave, Inlet { link, alive })
    }

    /// 同じプロセスの中の配り手（Origin、または中継する Slave）に入る。
    /// 吸い上げる係を裏で立てるので、tokio の中で呼ぶ
    pub fn follow(source: &impl Source) -> Slave {
        let (snapshot, mut subscription) = source.subscribe();
        let (slave, inlet) = Slave::new(snapshot, source.upstream());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    next = subscription.next() => match next {
                        Some(updates) => {
                            for update in &updates {
                                inlet.apply(update);
                            }
                        }
                        // 配り手が閉じた
                        None => break,
                    },
                    // Slave が手放された
                    _ = inlet.slave_gone() => break,
                }
            }
            // ここで inlet が落ち、Slave と下流に「外れた」が伝わる
        });
        slave
    }

    /// 何代めの Origin の写しか
    pub fn life(&self) -> u64 {
        self.link.shared.lock().life()
    }

    /// 手元の、ある要素の今の値
    pub fn get(&self, slot: &str) -> Option<SlotState> {
        self.link.shared.lock().get(slot).cloned()
    }

    /// 手元の状態を丸ごと
    pub fn snapshot(&self) -> Snapshot {
        self.link.shared.lock().snapshot()
    }

    /// 手元の変動を待つ。前回から変わった要素の、今の値を返す（途中の rev は畳まれる）。
    /// 上流から外れたら、残りを返したあとに `None`
    pub async fn changed(&mut self) -> Option<Vec<SlotUpdate>> {
        self.watch.next().await
    }

    /// Origin に置き換えを頼む。確定した変動を返し、手元にも合流させる
    pub async fn set(&self, slot: &str, value: Value) -> Result<SlotUpdate, SyncError> {
        Upstream::set(&*self.link, slot.to_string(), value).await
    }

    /// 上流から外れたか
    pub fn is_closed(&self) -> bool {
        self.link.shared.is_closed()
    }
}

/// 中継: Slave は、そのまま下流の Slave への配り手になれる
impl Source for Slave {
    fn subscribe(&self) -> (Snapshot, Subscription) {
        self.link.shared.subscribe()
    }

    fn upstream(&self) -> Arc<dyn Upstream> {
        self.link.clone()
    }
}

#[cfg(test)]
mod tests {
    //! 名前は、できるだけ `docs/model/field.qnt` の場面の確かめに揃えてある。

    use std::time::Duration;

    use serde_json::json;

    use super::*;

    const PATIENCE: Duration = Duration::from_secs(5);

    fn update(life: u64, slot: &str, rev: u64, value: Value) -> SlotUpdate {
        SlotUpdate {
            life,
            slot: slot.into(),
            rev,
            value,
        }
    }

    fn state(rev: u64, value: Value) -> SlotState {
        SlotState { rev, value }
    }

    /// 条件が成り立つまで、ほかのタスクに順番を譲りながら待つ
    async fn until(what: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(PATIENCE, async {
            while !done() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("時間切れ: {what}"));
    }

    async fn within<T>(what: &str, work: impl Future<Output = T>) -> T {
        tokio::time::timeout(PATIENCE, work)
            .await
            .unwrap_or_else(|_| panic!("時間切れ: {what}"))
    }

    // ── Slots ──

    #[test]
    fn set_advances_rev_per_slot() {
        let mut slots = Slots::new(1);
        assert_eq!(
            slots.set("light", json!(1)),
            update(1, "light", 1, json!(1))
        );
        assert_eq!(
            slots.set("light", json!(2)),
            update(1, "light", 2, json!(2))
        );
        assert_eq!(
            slots.set("camera", json!("wide")),
            update(1, "camera", 1, json!("wide"))
        );
        assert_eq!(slots.get("light"), Some(&state(2, json!(2))));
        assert_eq!(slots.get("camera"), Some(&state(1, json!("wide"))));
        assert_eq!(slots.get("sound"), None);
    }

    #[test]
    fn out_of_order_is_ignored() {
        let mut slots = Slots::new(1);
        assert!(slots.merge(&update(1, "light", 2, json!("b"))));
        assert!(!slots.merge(&update(1, "light", 1, json!("a"))));
        // 同じ rev も、何も変えない
        assert!(!slots.merge(&update(1, "light", 2, json!("b"))));
        assert_eq!(slots.get("light"), Some(&state(2, json!("b"))));
    }

    #[test]
    fn stale_life_is_ignored() {
        let mut slots = Slots::new(2);
        assert!(!slots.merge(&update(1, "light", 5, json!("old"))));
        assert_eq!(slots.get("light"), None);
        assert!(slots.merge(&update(2, "light", 1, json!("new"))));
    }

    #[test]
    fn rev_zero_is_ignored() {
        let mut slots = Slots::new(1);
        assert!(!slots.merge(&update(1, "light", 0, json!(1))));
        assert_eq!(slots.get("light"), None);
    }

    #[test]
    fn slots_merge_in_any_order() {
        let mut origin = Slots::new(1);
        let light = origin.set("light", json!(1));
        let camera = origin.set("camera", json!(2));
        let mut slave = Slots::new(1);
        assert!(slave.merge(&camera));
        assert!(slave.merge(&light));
        assert_eq!(slave.snapshot(), origin.snapshot());
    }

    #[test]
    fn changes_since_folds_to_latest_in_change_order() {
        let mut slots = Slots::new(1);
        slots.set("light", json!(1));
        let since = slots.seq();
        slots.set("camera", json!(1));
        slots.set("light", json!(2));
        slots.set("light", json!(3));
        assert_eq!(
            slots.changes_since(since),
            [
                update(1, "camera", 1, json!(1)),
                update(1, "light", 3, json!(3)),
            ]
        );
        assert_eq!(slots.changes_since(slots.seq()), []);
    }

    #[test]
    fn snapshot_round_trips() {
        let mut slots = Slots::new(7);
        slots.set("light", json!(1));
        slots.set("camera", json!({ "x": 1 }));
        let again = Slots::from_snapshot(slots.snapshot());
        assert_eq!(again.snapshot(), slots.snapshot());
        assert_eq!(again.life(), 7);
    }

    // ── Origin と購読 ──

    #[tokio::test]
    async fn subscribe_gets_snapshot_then_updates() {
        let origin = Origin::new(1);
        origin.set("light", json!(1)).unwrap();
        let (snapshot, mut subscription) = origin.subscribe();
        assert_eq!(snapshot.slots.get("light"), Some(&state(1, json!(1))));
        origin.set("light", json!(2)).unwrap();
        assert_eq!(
            within("変動", subscription.next()).await,
            Some(vec![update(1, "light", 2, json!(2))])
        );
    }

    #[tokio::test]
    async fn slow_subscriber_gets_latest_per_slot() {
        let origin = Origin::new(1);
        let (_, mut subscription) = origin.subscribe();
        for i in 1..=5 {
            origin.set("light", json!(i)).unwrap();
        }
        origin.set("camera", json!("wide")).unwrap();
        // 溜まったのは要素の数（2）まで。light の途中の rev は畳まれている
        assert_eq!(
            within("変動", subscription.next()).await,
            Some(vec![
                update(1, "light", 5, json!(5)),
                update(1, "camera", 1, json!("wide")),
            ])
        );
    }

    #[tokio::test]
    async fn close_ends_subscription_after_pending_changes() {
        let origin = Origin::new(1);
        let (_, mut subscription) = origin.subscribe();
        origin.set("light", json!(1)).unwrap();
        origin.close();
        assert!(origin.is_closed());
        assert_eq!(
            within("残りの変動", subscription.next()).await,
            Some(vec![update(1, "light", 1, json!(1))])
        );
        assert_eq!(within("終わり", subscription.next()).await, None);
    }

    #[test]
    fn cannot_set_closed() {
        let origin = Origin::new(1);
        origin.close();
        assert_eq!(origin.set("light", json!(1)), Err(SyncError::Closed));
    }

    // ── Slave ──

    #[tokio::test]
    async fn join_gets_snapshot_then_updates() {
        let origin = Origin::new(3);
        origin.set("light", json!(1)).unwrap();
        let mut slave = Slave::follow(&origin);
        assert_eq!(slave.life(), 3);
        assert_eq!(slave.get("light"), Some(state(1, json!(1))));
        origin.set("camera", json!("wide")).unwrap();
        assert_eq!(
            within("変動", slave.changed()).await,
            Some(vec![update(3, "camera", 1, json!("wide"))])
        );
        assert_eq!(slave.snapshot(), origin.snapshot());
    }

    #[tokio::test]
    async fn slave_set_is_confirmed_by_origin() {
        let origin = Origin::new(1);
        let writer = Slave::follow(&origin);
        let mut other = Slave::follow(&origin);
        let confirmed = within("頼む", writer.set("light", json!(2))).await.unwrap();
        assert_eq!(confirmed, update(1, "light", 1, json!(2)));
        // 頼んだ側は、変動の知らせを待たずに確定した値を持つ
        assert_eq!(writer.get("light"), Some(state(1, json!(2))));
        assert_eq!(origin.get("light"), Some(state(1, json!(2))));
        // ほかの Slave にも届く
        assert_eq!(within("変動", other.changed()).await, Some(vec![confirmed]));
    }

    #[tokio::test]
    async fn origin_decides_concurrent_sets() {
        let origin = Origin::new(1);
        let a = Slave::follow(&origin);
        let b = Slave::follow(&origin);
        let (first, second) = tokio::join!(a.set("light", json!("a")), b.set("light", json!("b")));
        let (first, second) = (first.unwrap(), second.unwrap());
        // 後から確定したほうが勝つ
        let winner = if first.rev > second.rev {
            first
        } else {
            second
        };
        assert_eq!(winner.rev, 2);
        assert_eq!(origin.get("light"), Some(state(2, winner.value.clone())));
        until("全員が揃う", || {
            a.get("light") == origin.get("light") && b.get("light") == origin.get("light")
        })
        .await;
    }

    /// 決まった返事をする上流（応答の道を手で動かす）
    struct Canned(SlotUpdate);

    impl Upstream for Canned {
        fn set(&self, _slot: String, _value: Value) -> SetFuture<'_> {
            Box::pin(std::future::ready(Ok(self.0.clone())))
        }
    }

    #[tokio::test]
    async fn late_reply_is_ignored() {
        let (slave, inlet) = Slave::new(
            Snapshot {
                life: 1,
                slots: BTreeMap::new(),
            },
            Arc::new(Canned(update(1, "light", 1, json!("a")))),
        );
        // 応答より先に、後の変動の知らせが届いた
        assert!(inlet.apply(&update(1, "light", 2, json!("b"))));
        let confirmed = slave.set("light", json!("a")).await.unwrap();
        assert_eq!(confirmed.rev, 1);
        assert_eq!(slave.get("light"), Some(state(2, json!("b"))));
    }

    #[tokio::test]
    async fn reply_first_then_feed() {
        let (mut slave, inlet) = Slave::new(
            Snapshot {
                life: 1,
                slots: BTreeMap::new(),
            },
            Arc::new(Canned(update(1, "light", 1, json!("a")))),
        );
        slave.set("light", json!("a")).await.unwrap();
        assert_eq!(slave.get("light"), Some(state(1, json!("a"))));
        // 後から届いた同じ rev の知らせは、何も変えない
        assert!(!inlet.apply(&update(1, "light", 1, json!("a"))));
        assert_eq!(
            within("変動", slave.changed()).await,
            Some(vec![update(1, "light", 1, json!("a"))])
        );
        inlet.close();
        assert_eq!(within("終わり", slave.changed()).await, None);
    }

    /// 返事をしない上流（届かなくなった上流の代わり）
    struct Silent;

    impl Upstream for Silent {
        fn set(&self, _slot: String, _value: Value) -> SetFuture<'_> {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test]
    async fn pending_set_ends_when_slave_is_closed() {
        let (slave, inlet) = Slave::new(Snapshot::default(), Arc::new(Silent));
        let slave = Arc::new(slave);
        let pending = tokio::spawn({
            let slave = slave.clone();
            async move { slave.set("light", json!(1)).await }
        });
        tokio::task::yield_now().await;
        // 上流から外れたら、返事を待っている頼みも終わる（上流の返事を待ち続けない）
        inlet.close();
        assert_eq!(
            within("頼みが終わる", pending).await.unwrap(),
            Err(SyncError::Closed)
        );
    }

    /// 返事をせず、何度 poll されたかを数える上流
    struct Counting(Arc<std::sync::atomic::AtomicUsize>);

    impl Upstream for Counting {
        fn set(&self, _slot: String, _value: Value) -> SetFuture<'_> {
            let polls = self.0.clone();
            Box::pin(std::future::poll_fn(move |_| {
                polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::task::Poll::Pending
            }))
        }
    }

    #[tokio::test]
    async fn pending_set_is_not_woken_by_updates() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let polls = Arc::new(AtomicUsize::new(0));
        let snapshot = Snapshot {
            life: 1,
            ..Snapshot::default()
        };
        let (slave, inlet) = Slave::new(snapshot, Arc::new(Counting(polls.clone())));
        let slave = Arc::new(slave);
        let pending = tokio::spawn({
            let slave = slave.clone();
            async move { slave.set("light", json!(0)).await }
        });
        until("頼みが上流へ出る", || {
            polls.load(Ordering::Relaxed) > 0
        })
        .await;
        let before = polls.load(Ordering::Relaxed);
        for rev in 1..=100 {
            assert!(inlet.apply(&update(1, "camera", rev, json!(rev))));
            tokio::task::yield_now().await;
        }
        // 返事を待つ頼みは、変動の知らせでは起きない（何千もの頼みが待っていても、変動のたびに全部は起きない）
        assert_eq!(polls.load(Ordering::Relaxed), before);
        inlet.close();
        assert_eq!(
            within("頼みが終わる", pending).await.unwrap(),
            Err(SyncError::Closed)
        );
    }

    #[tokio::test]
    async fn pending_relayed_set_ends_when_relay_leaves() {
        let (relay, inlet) = Slave::new(Snapshot::default(), Arc::new(Silent));
        // 受け取り係の代わり: 中継が手放されたら、上流から外れる
        tokio::spawn(async move { inlet.slave_gone().await });
        let downstream = Arc::new(Slave::follow(&relay));
        let pending = tokio::spawn({
            let downstream = downstream.clone();
            async move { downstream.set("light", json!(1)).await }
        });
        tokio::task::yield_now().await;
        // 中継が手放されると、中継を経て返事を待っていた下流の頼みも終わる
        drop(relay);
        assert_eq!(
            within("頼みが終わる", pending).await.unwrap(),
            Err(SyncError::Closed)
        );
    }

    #[tokio::test]
    async fn stale_life_update_is_ignored_by_slave() {
        let (slave, inlet) = Slave::new(
            Snapshot {
                life: 2,
                slots: BTreeMap::new(),
            },
            Arc::new(Canned(update(2, "light", 1, json!(1)))),
        );
        assert!(!inlet.apply(&update(1, "light", 9, json!("old"))));
        assert_eq!(slave.get("light"), None);
    }

    #[tokio::test]
    async fn slow_consumer_slave_is_always_current() {
        let origin = Origin::new(1);
        let mut slave = Slave::follow(&origin);
        // 手元の利用者は changed() を呼ばないまま、Origin が 100 回置き換わる
        for i in 1..=100 {
            origin.set("light", json!(i)).unwrap();
            tokio::task::yield_now().await;
        }
        until("Slave が追いつく", || {
            slave.get("light") == Some(state(100, json!(100)))
        })
        .await;
        // 利用者が見に来たときには、最新だけが 1 つ
        assert_eq!(
            within("変動", slave.changed()).await,
            Some(vec![update(1, "light", 100, json!(100))])
        );
    }

    // ── 中継 ──

    #[tokio::test]
    async fn relay_delivers_downstream() {
        let origin = Origin::new(1);
        let relay = Slave::follow(&origin);
        let mut downstream = Slave::follow(&relay);
        origin.set("light", json!(1)).unwrap();
        // rev は Origin が振ったまま
        assert_eq!(
            within("変動", downstream.changed()).await,
            Some(vec![update(1, "light", 1, json!(1))])
        );
    }

    #[tokio::test]
    async fn relay_chain() {
        let origin = Origin::new(1);
        let first = Slave::follow(&origin);
        let second = Slave::follow(&first);
        let mut third = Slave::follow(&second);
        origin.set("camera", json!(2)).unwrap();
        assert_eq!(
            within("変動", third.changed()).await,
            Some(vec![update(1, "camera", 1, json!(2))])
        );
    }

    #[tokio::test]
    async fn relay_join_gets_relay_state() {
        let origin = Origin::new(1);
        origin.set("light", json!(1)).unwrap();
        let relay = Slave::follow(&origin);
        let downstream = Slave::follow(&relay);
        assert_eq!(downstream.snapshot(), relay.snapshot());
    }

    #[tokio::test]
    async fn relayed_slave_can_set() {
        let origin = Origin::new(1);
        let relay = Slave::follow(&origin);
        let downstream = Slave::follow(&relay);
        let confirmed = within("頼む", downstream.set("light", json!(2)))
            .await
            .unwrap();
        assert_eq!(confirmed, update(1, "light", 1, json!(2)));
        // 頼みは中継を経て Origin で確定し、応答の道で中継にも頼んだ人にも合流している
        assert_eq!(origin.get("light"), Some(state(1, json!(2))));
        assert_eq!(relay.get("light"), Some(state(1, json!(2))));
        assert_eq!(downstream.get("light"), Some(state(1, json!(2))));
    }

    #[tokio::test]
    async fn relay_leave_drops_downstream() {
        let origin = Origin::new(1);
        let relay = Slave::follow(&origin);
        let mut downstream = Slave::follow(&relay);
        drop(relay);
        assert_eq!(within("外れる", downstream.changed()).await, None);
        assert!(downstream.is_closed());
        assert_eq!(
            downstream.set("light", json!(1)).await,
            Err(SyncError::Closed)
        );
    }

    #[tokio::test]
    async fn stop_drops_everyone() {
        let origin = Origin::new(1);
        let mut relay = Slave::follow(&origin);
        let mut downstream = Slave::follow(&relay);
        origin.close();
        assert_eq!(within("外れる", relay.changed()).await, None);
        assert_eq!(within("外れる", downstream.changed()).await, None);
    }
}
