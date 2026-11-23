//! field そのもの。IO を持たない。
//!
//! `docs/model/field.qnt` のモデルを写したもの。操作と性質の名前は
//! モデルに揃えてある（テストの名前は、モデルの場面の確かめと同じ）。
//!
//! 要素（slot）と、その同期は、汎用の [`crate::sync`] に任せる。field が持つのは、
//! 持ち主・名前・場所という名札と、動いているか、誰が入っているか、だけ。
//!
//! モデルとの対応:
//! - `registry` の 1 件        → [`Field`]（要素は中の [`Origin`] が持つ）
//! - `isJoined` / `joined`     → [`Field::visitors`]
//! - `join` / `leave`          → [`Field::join`] / [`Field::leave`]
//! - `setSlot` / `commit`      → [`Field::set`]（どちらも Origin で確定する。頼みが届く道は protocol）
//! - `stop` / `restart`        → [`Field::stop`] / [`Field::restart`]
//!
//! 知らせが届くまでの道のり（`inflight` / `receive`）と、入った人の手元の Slave はここには無い。
//! それは runtime / protocol と、入ってくる側（client）の仕事。

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::sync::{self, Origin, SlotState, SlotUpdate, Subscription};

/// field を指す鍵。持ち主の中で名前は一意
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FieldKey {
    pub owner: String,
    pub name: String,
}

impl FieldKey {
    pub fn new(owner: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for FieldKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.owner, self.name)
    }
}

/// field が住む場所。生まれた場所に住み続ける
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Place {
    Local,
    Cloud,
}

/// 入っている人を見分ける番号（接続ごとに 1 つ）
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VisitorId(pub u64);

/// field の名札と代
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldInfo {
    pub owner: String,
    pub name: String,
    pub place: Place,
    /// 何代めか。立ち上がり直すたびに大きくなる
    pub life: u64,
}

/// ある時点の field の状態: 名札と、要素ごとの `(rev, value)`。`Join` の応答の中身
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldSnapshot {
    pub owner: String,
    pub name: String,
    pub place: Place,
    pub life: u64,
    pub slots: BTreeMap<String, SlotState>,
}

impl FieldSnapshot {
    /// 名札と状態を合わせる
    pub fn new(info: FieldInfo, state: sync::Snapshot) -> Self {
        debug_assert_eq!(info.life, state.life);
        Self {
            owner: info.owner,
            name: info.name,
            place: info.place,
            life: info.life,
            slots: state.slots,
        }
    }

    pub fn info(&self) -> FieldInfo {
        FieldInfo {
            owner: self.owner.clone(),
            name: self.name.clone(),
            place: self.place,
            life: self.life,
        }
    }

    /// 名札を外した、同期の層の状態
    pub fn into_sync(self) -> sync::Snapshot {
        sync::Snapshot {
            life: self.life,
            slots: self.slots,
        }
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum FieldError {
    #[error("field は止まっている")]
    Stopped,
    #[error("field はすでに動いている")]
    AlreadyRunning,
    #[error("すでに入っている")]
    AlreadyJoined,
    #[error("入っていない")]
    NotJoined,
    #[error("要素の名前が空")]
    EmptySlotName,
    #[error("代は前より大きくなければならない（前 {previous}、今回 {given}）")]
    LifeNotNewer { previous: u64, given: u64 },
}

/// field 1 つ
pub struct Field {
    key: FieldKey,
    place: Place,
    /// 要素（正本）と代
    origin: Origin,
    running: bool,
    visitors: BTreeSet<VisitorId>,
}

impl Field {
    /// 持つ: field を作る。作った時点で動いている。要素はまだ空
    pub fn new(key: FieldKey, place: Place, life: u64) -> Self {
        Self {
            key,
            place,
            origin: Origin::new(life),
            running: true,
            visitors: BTreeSet::new(),
        }
    }

    pub fn key(&self) -> &FieldKey {
        &self.key
    }

    pub fn place(&self) -> Place {
        self.place
    }

    pub fn life(&self) -> u64 {
        self.origin.life()
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    /// 入っている人
    pub fn visitors(&self) -> &BTreeSet<VisitorId> {
        &self.visitors
    }

    pub fn info(&self) -> FieldInfo {
        FieldInfo {
            owner: self.key.owner.clone(),
            name: self.key.name.clone(),
            place: self.place,
            life: self.life(),
        }
    }

    pub fn snapshot(&self) -> FieldSnapshot {
        FieldSnapshot::new(self.info(), self.origin.snapshot())
    }

    /// ある要素の今の値
    pub fn get(&self, slot: &str) -> Option<SlotState> {
        self.origin.get(slot)
    }

    /// 入る: その時点の状態と、以後の変動の購読を受け取る
    pub fn join(
        &mut self,
        visitor: VisitorId,
    ) -> Result<(FieldSnapshot, Subscription), FieldError> {
        if !self.running {
            return Err(FieldError::Stopped);
        }
        if !self.visitors.insert(visitor) {
            return Err(FieldError::AlreadyJoined);
        }
        let (state, subscription) = self.origin.subscribe();
        Ok((FieldSnapshot::new(self.info(), state), subscription))
    }

    /// 入らずに見る: 以後の変動の購読（持ち主の手元の表示など。入っている人には数えない）
    pub fn observe(&self) -> Subscription {
        self.origin.subscribe().1
    }

    /// 出ていく
    pub fn leave(&mut self, visitor: VisitorId) -> Result<(), FieldError> {
        if self.visitors.remove(&visitor) {
            Ok(())
        } else {
            Err(FieldError::NotJoined)
        }
    }

    /// 置き換える: 要素の値を置き換えて確定する（その要素の rev が 1 つ進む）。
    /// 持ち主が直接置き換えるときも、入っている人から頼まれたときも、ここを通る
    pub fn set(&mut self, slot: &str, value: Value) -> Result<SlotUpdate, FieldError> {
        if !self.running {
            return Err(FieldError::Stopped);
        }
        if slot.is_empty() {
            return Err(FieldError::EmptySlotName);
        }
        self.origin
            .set(slot, value)
            .map_err(|_| FieldError::Stopped)
    }

    /// 止まる: 入っていた人は全員外れる（購読は、残りの変動を受け取ったあとに終わる）
    pub fn stop(&mut self) -> Result<(), FieldError> {
        if !self.running {
            return Err(FieldError::Stopped);
        }
        self.running = false;
        self.visitors.clear();
        self.origin.close();
        Ok(())
    }

    /// 立ち上がり直す: 同じ field が、次の代として動き出す。要素は空から
    pub fn restart(&mut self, life: u64) -> Result<(), FieldError> {
        if self.running {
            return Err(FieldError::AlreadyRunning);
        }
        if life <= self.life() {
            return Err(FieldError::LifeNotNewer {
                previous: self.life(),
                given: life,
            });
        }
        self.origin = Origin::new(life);
        self.running = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! 名前は `docs/model/field.qnt` の場面の確かめと同じ。
    //! 複数の field にまたがる場面は host.rs のテストにある。

    use serde_json::json;

    use super::*;

    fn mako_studio() -> Field {
        Field::new(FieldKey::new("mako", "studio"), Place::Local, 1)
    }

    fn state(rev: u64, value: Value) -> SlotState {
        SlotState { rev, value }
    }

    const V1: VisitorId = VisitorId(1);
    const V2: VisitorId = VisitorId(2);

    #[test]
    fn set_advances_with_nobody() {
        let mut field = mako_studio();
        field.set("light", json!(1)).unwrap();
        field.set("light", json!(2)).unwrap();
        let update = field.set("camera", json!(1)).unwrap();
        assert_eq!(field.get("light"), Some(state(2, json!(2))));
        assert_eq!(update.rev, 1);
        assert!(field.visitors().is_empty());
    }

    #[test]
    fn join_gets_snapshot() {
        let mut field = mako_studio();
        field.set("light", json!(1)).unwrap();
        let (snapshot, _subscription) = field.join(V1).unwrap();
        assert_eq!(snapshot, field.snapshot());
        assert_eq!(
            snapshot,
            FieldSnapshot {
                owner: "mako".into(),
                name: "studio".into(),
                place: Place::Local,
                life: 1,
                slots: BTreeMap::from([("light".into(), state(1, json!(1)))]),
            }
        );
        assert!(field.visitors().contains(&V1));
    }

    #[test]
    fn snapshot_carries_place_and_life() {
        let mut field = Field::new(FieldKey::new("mako", "studio"), Place::Cloud, 7);
        let update = field.set("light", json!(1)).unwrap();
        assert_eq!(update.life, 7);
        let snapshot = field.snapshot();
        assert_eq!((snapshot.place, snapshot.life), (Place::Cloud, 7));
    }

    #[test]
    fn slots_are_independent() {
        let mut field = mako_studio();
        field.set("light", json!(1)).unwrap();
        field.set("camera", json!(2)).unwrap();
        field.set("light", json!(2)).unwrap();
        assert_eq!(field.get("light").unwrap().rev, 2);
        assert_eq!(field.get("camera").unwrap().rev, 1);
    }

    #[test]
    fn leave_stops_delivery() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        field.join(V2).unwrap();
        field.leave(V1).unwrap();
        assert_eq!(field.visitors().iter().copied().collect::<Vec<_>>(), [V2]);
        // field は動き続ける
        assert!(field.set("light", json!(1)).is_ok());
    }

    #[test]
    fn rejoin_gets_current_snapshot() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        field.set("light", json!(1)).unwrap();
        field.leave(V1).unwrap();
        field.set("light", json!(2)).unwrap();
        field.set("light", json!(3)).unwrap();
        let (snapshot, _subscription) = field.join(V1).unwrap();
        assert_eq!(snapshot.slots.get("light"), Some(&state(3, json!(3))));
    }

    #[test]
    fn join_twice_fails() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        assert!(matches!(field.join(V1), Err(FieldError::AlreadyJoined)));
    }

    #[test]
    fn leave_without_join_fails() {
        let mut field = mako_studio();
        assert_eq!(field.leave(V1), Err(FieldError::NotJoined));
    }

    #[test]
    fn empty_slot_name_fails() {
        let mut field = mako_studio();
        assert_eq!(field.set("", json!(1)), Err(FieldError::EmptySlotName));
    }

    #[test]
    fn stop_drops_everyone() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        field.join(V2).unwrap();
        field.stop().unwrap();
        assert!(field.visitors().is_empty());
        assert!(!field.is_running());
    }

    #[test]
    fn cannot_join_stopped() {
        let mut field = mako_studio();
        field.stop().unwrap();
        assert!(matches!(field.join(V1), Err(FieldError::Stopped)));
    }

    #[test]
    fn cannot_set_stopped() {
        let mut field = mako_studio();
        field.stop().unwrap();
        assert_eq!(field.set("light", json!(1)), Err(FieldError::Stopped));
    }

    #[test]
    fn cannot_stop_stopped() {
        let mut field = mako_studio();
        field.stop().unwrap();
        assert_eq!(field.stop(), Err(FieldError::Stopped));
    }

    #[test]
    fn restart_keeps_identity() {
        let mut field = Field::new(FieldKey::new("mako", "studio"), Place::Cloud, 1);
        field.set("light", json!(1)).unwrap();
        field.set("camera", json!(2)).unwrap();
        field.stop().unwrap();
        field.restart(2).unwrap();
        assert_eq!(field.key(), &FieldKey::new("mako", "studio"));
        assert_eq!(field.place(), Place::Cloud);
        assert_eq!(field.life(), 2);
        assert!(field.snapshot().slots.is_empty());
        assert!(field.is_running());
    }

    #[test]
    fn rejoin_after_restart() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        field.set("light", json!(1)).unwrap();
        field.stop().unwrap();
        field.restart(2).unwrap();
        let (snapshot, _subscription) = field.join(V1).unwrap();
        assert_eq!(snapshot.life, 2);
        assert!(snapshot.slots.is_empty());
        let update = field.set("light", json!(2)).unwrap();
        assert_eq!((update.life, update.rev), (2, 1));
    }

    #[test]
    fn cannot_restart_running() {
        let mut field = mako_studio();
        assert_eq!(field.restart(2), Err(FieldError::AlreadyRunning));
    }

    #[test]
    fn restart_needs_newer_life() {
        let mut field = Field::new(FieldKey::new("mako", "studio"), Place::Local, 5);
        field.stop().unwrap();
        assert_eq!(
            field.restart(5),
            Err(FieldError::LifeNotNewer {
                previous: 5,
                given: 5
            })
        );
        assert!(!field.is_running());
    }
}
