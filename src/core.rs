//! field そのもの。IO を持たない。
//!
//! `docs/model/field.qnt` のモデルを写したもの。操作と性質の名前は
//! モデルに揃えてある（テストの名前は、モデルの場面の確かめと同じ）。
//!
//! モデルとの対応:
//! - `registry` の 1 件      → [`Field`]
//! - `isJoined` / `joined`   → [`Field::visitors`]
//! - `join` / `leave`        → [`Field::join`] / [`Field::leave`]
//! - `tick`                  → [`Field::tick`]
//! - `stop` / `restart`      → [`Field::stop`] / [`Field::restart`]
//!
//! 知らせが届くまでの道のり（`inflight` / `receive`）はここには無い。
//! それは runtime と、入ってくる側の仕事。

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

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

/// ある時点の field の状態。`Join` の応答と、tick の知らせの中身
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub owner: String,
    pub name: String,
    pub place: Place,
    /// 何代めか。立ち上がり直すたびに大きくなる
    pub life: u64,
    /// field の時計
    pub tick: u64,
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
    #[error("代は前より大きくなければならない（前 {previous}、今回 {given}）")]
    LifeNotNewer { previous: u64, given: u64 },
}

/// field 1 つ
#[derive(Clone, Debug)]
pub struct Field {
    key: FieldKey,
    place: Place,
    life: u64,
    tick: u64,
    running: bool,
    visitors: BTreeSet<VisitorId>,
}

impl Field {
    /// 持つ: field を作る。作った時点で動いている
    pub fn new(key: FieldKey, place: Place, life: u64) -> Self {
        Self {
            key,
            place,
            life,
            tick: 0,
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
        self.life
    }

    pub fn current_tick(&self) -> u64 {
        self.tick
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    /// 入っている人
    pub fn visitors(&self) -> &BTreeSet<VisitorId> {
        &self.visitors
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            owner: self.key.owner.clone(),
            name: self.key.name.clone(),
            place: self.place,
            life: self.life,
            tick: self.tick,
        }
    }

    /// 入る: その時点の状態を受け取る
    pub fn join(&mut self, visitor: VisitorId) -> Result<Snapshot, FieldError> {
        if !self.running {
            return Err(FieldError::Stopped);
        }
        if !self.visitors.insert(visitor) {
            return Err(FieldError::AlreadyJoined);
        }
        Ok(self.snapshot())
    }

    /// 出ていく
    pub fn leave(&mut self, visitor: VisitorId) -> Result<(), FieldError> {
        if self.visitors.remove(&visitor) {
            Ok(())
        } else {
            Err(FieldError::NotJoined)
        }
    }

    /// tick: 時計が 1 つ進む。返すのは、入っている人へ送り出す知らせの中身
    pub fn tick(&mut self) -> Result<Snapshot, FieldError> {
        if !self.running {
            return Err(FieldError::Stopped);
        }
        self.tick += 1;
        Ok(self.snapshot())
    }

    /// 止まる: 入っていた人は全員外れる
    pub fn stop(&mut self) -> Result<(), FieldError> {
        if !self.running {
            return Err(FieldError::Stopped);
        }
        self.running = false;
        self.visitors.clear();
        Ok(())
    }

    /// 立ち上がり直す: 同じ field が、次の代として動き出す。時計は 0 から
    pub fn restart(&mut self, life: u64) -> Result<(), FieldError> {
        if self.running {
            return Err(FieldError::AlreadyRunning);
        }
        if life <= self.life {
            return Err(FieldError::LifeNotNewer {
                previous: self.life,
                given: life,
            });
        }
        self.life = life;
        self.tick = 0;
        self.running = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! 名前は `docs/model/field.qnt` の場面の確かめと同じ。
    //! 複数の field にまたがる場面は host.rs のテストにある。

    use super::*;

    fn mako_studio() -> Field {
        Field::new(FieldKey::new("mako", "studio"), Place::Local, 1)
    }

    const V1: VisitorId = VisitorId(1);
    const V2: VisitorId = VisitorId(2);

    #[test]
    fn tick_advances_with_nobody() {
        let mut field = mako_studio();
        field.tick().unwrap();
        field.tick().unwrap();
        let note = field.tick().unwrap();
        assert_eq!(field.current_tick(), 3);
        assert_eq!(note.tick, 3);
        assert!(field.visitors().is_empty());
    }

    #[test]
    fn join_gets_snapshot() {
        let mut field = mako_studio();
        field.tick().unwrap();
        field.tick().unwrap();
        let snapshot = field.join(V1).unwrap();
        assert_eq!(
            snapshot,
            Snapshot {
                owner: "mako".into(),
                name: "studio".into(),
                place: Place::Local,
                life: 1,
                tick: 2,
            }
        );
        assert!(field.visitors().contains(&V1));
    }

    #[test]
    fn tick_note_carries_life_and_tick() {
        let mut field = Field::new(FieldKey::new("mako", "studio"), Place::Cloud, 7);
        let note = field.tick().unwrap();
        assert_eq!(note.life, 7);
        assert_eq!(note.tick, 1);
        assert_eq!(note.place, Place::Cloud);
    }

    #[test]
    fn leave_stops_delivery() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        field.join(V2).unwrap();
        field.leave(V1).unwrap();
        assert_eq!(field.visitors().iter().copied().collect::<Vec<_>>(), [V2]);
        // field は動き続ける
        field.tick().unwrap();
        assert_eq!(field.current_tick(), 1);
    }

    #[test]
    fn rejoin_gets_current_snapshot() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        field.tick().unwrap();
        field.leave(V1).unwrap();
        field.tick().unwrap();
        field.tick().unwrap();
        assert_eq!(field.join(V1).unwrap().tick, 3);
    }

    #[test]
    fn join_twice_fails() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        assert_eq!(field.join(V1), Err(FieldError::AlreadyJoined));
    }

    #[test]
    fn leave_without_join_fails() {
        let mut field = mako_studio();
        assert_eq!(field.leave(V1), Err(FieldError::NotJoined));
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
        assert_eq!(field.join(V1), Err(FieldError::Stopped));
    }

    #[test]
    fn cannot_tick_stopped() {
        let mut field = mako_studio();
        field.stop().unwrap();
        assert_eq!(field.tick(), Err(FieldError::Stopped));
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
        field.tick().unwrap();
        field.tick().unwrap();
        field.stop().unwrap();
        field.restart(2).unwrap();
        assert_eq!(field.key(), &FieldKey::new("mako", "studio"));
        assert_eq!(field.place(), Place::Cloud);
        assert_eq!(field.life(), 2);
        assert_eq!(field.current_tick(), 0);
        assert!(field.is_running());
    }

    #[test]
    fn rejoin_after_restart() {
        let mut field = mako_studio();
        field.join(V1).unwrap();
        field.tick().unwrap();
        field.stop().unwrap();
        field.restart(2).unwrap();
        let snapshot = field.join(V1).unwrap();
        assert_eq!((snapshot.life, snapshot.tick), (2, 0));
        assert_eq!(field.tick().unwrap().tick, 1);
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
