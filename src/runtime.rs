//! 1 つの field を 1 つのタスクで動かす。
//!
//! タスクが [`Field`]（core）を持ち、入る・出ていく・置き換える・止まる、の頼みを順に受ける。
//! 時計は無い。変動は、要素が置き換えられたときだけ起きる（変動駆動）。
//!
//! ## 「新しいものだけ」の作り
//!
//! 入っている人へは、[`Subscription`]（sync）で変動を渡す。購読は、前回から変わった要素の
//! 今の値だけを返す（途中の rev は畳まれる）。受け取りが遅い人に溜まるのは要素の数まで。
//! モデルの `streamLatest`（要素ごとに順番どおり・新しいものだけ）に当たる。
//! 古いものが畳まれるのは購読の中、つまり stream へ書き込む前。

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::core::{Field, FieldError, FieldInfo, FieldKey, FieldSnapshot, VisitorId};
use crate::sync::{SlotUpdate, Subscription};

/// ある時点の field の様子（確かめ用）
#[derive(Clone, Debug, PartialEq)]
pub struct FieldStatus {
    pub snapshot: FieldSnapshot,
    /// 入っている人の数
    pub visitors: usize,
}

enum Command {
    Join {
        reply: oneshot::Sender<Result<(FieldSnapshot, Presence), FieldError>>,
    },
    Leave {
        visitor: VisitorId,
    },
    Set {
        slot: String,
        value: Value,
        reply: oneshot::Sender<Result<SlotUpdate, FieldError>>,
    },
    Observe {
        reply: oneshot::Sender<Subscription>,
    },
    Status {
        reply: oneshot::Sender<FieldStatus>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
}

/// 動いている field への口。複製して配ってよい
#[derive(Clone)]
pub struct FieldHandle {
    key: FieldKey,
    commands: mpsc::UnboundedSender<Command>,
}

/// field に入っている、ということ。手放す（drop する）と出ていく
pub struct Presence {
    visitor: VisitorId,
    info: FieldInfo,
    /// 入った時点より後の変動
    updates: Subscription,
    commands: mpsc::UnboundedSender<Command>,
}

impl Presence {
    pub fn visitor(&self) -> VisitorId {
        self.visitor
    }

    pub fn info(&self) -> &FieldInfo {
        &self.info
    }

    /// 次の変動を待つ。前回から変わった要素の今の値を、変わった順に返す（途中の rev は畳まれる）。
    /// field が止まったら `None`
    pub async fn next(&mut self) -> Option<Vec<SlotUpdate>> {
        self.updates.next().await
    }
}

impl Drop for Presence {
    fn drop(&mut self) {
        // field がもう止まっていれば届かないが、それでよい
        let _ = self.commands.send(Command::Leave {
            visitor: self.visitor,
        });
    }
}

impl FieldHandle {
    pub fn key(&self) -> &FieldKey {
        &self.key
    }

    /// 入る。入った時点の状態と、入っていること（以後の変動）を受け取る
    pub async fn join(&self) -> Result<(FieldSnapshot, Presence), FieldError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Join { reply })
            .map_err(|_| FieldError::Stopped)?;
        answer.await.map_err(|_| FieldError::Stopped)?
    }

    /// 置き換える: 要素の値を置き換えて確定する。確定した変動を返す
    pub async fn set(
        &self,
        slot: impl Into<String>,
        value: Value,
    ) -> Result<SlotUpdate, FieldError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Set {
                slot: slot.into(),
                value,
                reply,
            })
            .map_err(|_| FieldError::Stopped)?;
        answer.await.map_err(|_| FieldError::Stopped)?
    }

    /// 入らずに見る（持ち主の手元の表示など）。field が止まっていれば `None`
    pub async fn observe(&self) -> Option<Subscription> {
        let (reply, answer) = oneshot::channel();
        self.commands.send(Command::Observe { reply }).ok()?;
        answer.await.ok()
    }

    /// いまの様子。field が止まっていれば `None`
    pub async fn status(&self) -> Option<FieldStatus> {
        let (reply, answer) = oneshot::channel();
        self.commands.send(Command::Status { reply }).ok()?;
        answer.await.ok()
    }

    /// 止める。入っていた人は全員外れる。すでに止まっていれば何もしない
    pub async fn stop(&self) {
        let (reply, answer) = oneshot::channel();
        if self.commands.send(Command::Stop { reply }).is_ok() {
            let _ = answer.await;
        }
    }
}

/// field を動かし始める
pub fn spawn_field(mut field: Field) -> FieldHandle {
    let key = field.key().clone();
    let (commands, mut inbox) = mpsc::unbounded_channel::<Command>();
    let presence_commands = commands.downgrade();

    tokio::spawn(async move {
        let name = field.key().clone();
        let mut next_visitor: u64 = 1;
        tracing::info!("field {name}: 動き始めた（{}代め）", field.life());

        loop {
            match inbox.recv().await {
                Some(Command::Join { reply }) => {
                    let visitor = VisitorId(next_visitor);
                    next_visitor += 1;
                    // 口がもう無いなら、field は畳まれる途中
                    let Some(commands) = presence_commands.upgrade() else {
                        let _ = reply.send(Err(FieldError::Stopped));
                        continue;
                    };
                    let info = field.info();
                    let answer = field.join(visitor).map(|(snapshot, updates)| {
                        let presence = Presence {
                            visitor,
                            info,
                            updates,
                            commands,
                        };
                        (snapshot, presence)
                    });
                    // 受け取る側がもういなければ、Presence はここで手放され、
                    // その drop が「出ていく」を送ってくる
                    let _ = reply.send(answer);
                }
                Some(Command::Leave { visitor }) => {
                    let _ = field.leave(visitor);
                }
                Some(Command::Set { slot, value, reply }) => {
                    let _ = reply.send(field.set(&slot, value));
                }
                Some(Command::Observe { reply }) => {
                    let _ = reply.send(field.observe());
                }
                Some(Command::Status { reply }) => {
                    let _ = reply.send(FieldStatus {
                        snapshot: field.snapshot(),
                        visitors: field.visitors().len(),
                    });
                }
                Some(Command::Stop { reply }) => {
                    let _ = field.stop();
                    let _ = reply.send(());
                    break;
                }
                // 口が全部なくなった
                None => {
                    let _ = field.stop();
                    break;
                }
            }
        }
        // field.stop() が Origin を閉じたので、入っていた人の next は、残りを返したあとに None を返す
        tracing::info!("field {name}: 止まった");
    });

    FieldHandle { key, commands }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::core::Place;
    use crate::sync::SlotState;

    fn spawn() -> FieldHandle {
        spawn_field(Field::new(FieldKey::new("mako", "studio"), Place::Local, 1))
    }

    fn update(slot: &str, rev: u64, value: Value) -> SlotUpdate {
        SlotUpdate {
            life: 1,
            slot: slot.into(),
            rev,
            value,
        }
    }

    #[tokio::test]
    async fn set_advances_with_nobody() {
        let field = spawn();
        field.set("light", json!(1)).await.unwrap();
        field.set("light", json!(2)).await.unwrap();
        let status = field.status().await.unwrap();
        assert_eq!(
            status.snapshot.slots.get("light"),
            Some(&SlotState {
                rev: 2,
                value: json!(2)
            })
        );
        assert_eq!(status.visitors, 0);
    }

    #[tokio::test]
    async fn join_gets_snapshot_then_updates() {
        let field = spawn();
        field.set("light", json!(1)).await.unwrap();
        let (snapshot, mut presence) = field.join().await.unwrap();
        assert_eq!(snapshot.slots["light"].rev, 1);
        assert_eq!(presence.info().owner, "mako");
        field.set("camera", json!("wide")).await.unwrap();
        assert_eq!(
            presence.next().await,
            Some(vec![update("camera", 1, json!("wide"))])
        );
    }

    #[tokio::test]
    async fn slow_visitor_skips_to_recent() {
        let field = spawn();
        let (_, mut presence) = field.join().await.unwrap();
        // 受け取らないまま 5 回置き換わる
        for i in 1..=5 {
            field.set("light", json!(i)).await.unwrap();
        }
        // 溜まった 1〜4 は畳まれて、最新の 5 だけが来る
        assert_eq!(
            presence.next().await,
            Some(vec![update("light", 5, json!(5))])
        );
    }

    #[tokio::test]
    async fn updates_never_go_back_and_never_get_ahead() {
        let field = spawn();
        let (_, mut presence) = field.join().await.unwrap();
        let mut seen = 0;
        for i in 1..=10 {
            field.set("light", json!(i)).await.unwrap();
            for update in presence.next().await.unwrap() {
                assert!(update.rev > seen, "戻った: {seen} → {}", update.rev);
                seen = update.rev;
            }
            let now = field.status().await.unwrap().snapshot.slots["light"].rev;
            assert!(seen <= now, "先に進んだ: 手元 {seen}、field {now}");
        }
    }

    #[tokio::test]
    async fn two_visitors_both_get_updates() {
        let field = spawn();
        let (_, mut a) = field.join().await.unwrap();
        let (_, mut b) = field.join().await.unwrap();
        assert_ne!(a.visitor(), b.visitor());
        assert_eq!(field.status().await.unwrap().visitors, 2);
        field.set("light", json!(1)).await.unwrap();
        assert_eq!(a.next().await, Some(vec![update("light", 1, json!(1))]));
        assert_eq!(b.next().await, Some(vec![update("light", 1, json!(1))]));
    }

    #[tokio::test]
    async fn observe_is_not_a_visitor() {
        let field = spawn();
        let mut watching = field.observe().await.unwrap();
        assert_eq!(field.status().await.unwrap().visitors, 0);
        field.set("light", json!(1)).await.unwrap();
        assert_eq!(
            watching.next().await,
            Some(vec![update("light", 1, json!(1))])
        );
    }

    #[tokio::test]
    async fn leave_stops_delivery() {
        let field = spawn();
        let (_, presence) = field.join().await.unwrap();
        assert_eq!(field.status().await.unwrap().visitors, 1);
        drop(presence);
        assert_eq!(field.status().await.unwrap().visitors, 0);
        // field は動き続ける
        assert!(field.set("light", json!(1)).await.is_ok());
    }

    #[tokio::test]
    async fn stop_drops_everyone() {
        let field = spawn();
        let (_, mut presence) = field.join().await.unwrap();
        field.stop().await;
        // 入っていた人には「止まった」が伝わる
        assert_eq!(presence.next().await, None);
        assert!(field.status().await.is_none());
    }

    #[tokio::test]
    async fn cannot_join_stopped() {
        let field = spawn();
        field.stop().await;
        assert!(matches!(field.join().await, Err(FieldError::Stopped)));
    }

    #[tokio::test]
    async fn cannot_set_stopped() {
        let field = spawn();
        field.stop().await;
        assert_eq!(field.set("light", json!(1)).await, Err(FieldError::Stopped));
    }

    #[tokio::test]
    async fn stops_when_every_handle_is_gone() {
        let field = spawn();
        let mut watching = field.observe().await.unwrap();
        let (_, presence) = field.join().await.unwrap();
        drop(field);
        // Presence も口を 1 つ持っているので、まだ止まらない。それも手放すと止まる
        drop(presence);
        assert_eq!(watching.next().await, None);
    }
}
