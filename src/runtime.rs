//! 1 つの field を 1 つのタスクで動かす。
//!
//! タスクが [`Field`]（core）を持ち、一定間隔で時計を進め、入っている人へ
//! その時点の状態を送り出す。
//!
//! ## 「新しいものだけ」の作り
//!
//! 送り出しには `tokio::sync::watch` を使う。watch が持つのは**最新の 1 枚だけ**。
//! 受け取りが遅い人は、途中の tick を飛ばして最新のものだけを受け取る。
//! モデルの `streamLatest`（順番どおり・新しいものだけ）に当たる。
//! 古いものが捨てられるのは watch の中、つまり stream へ書き込む前。

use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};

use crate::core::{Field, FieldError, FieldKey, Snapshot, VisitorId};

/// field の動かし方
#[derive(Clone, Copy, Debug)]
pub struct FieldConfig {
    /// 時計の間隔
    pub tick_interval: Duration,
}

impl Default for FieldConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(1),
        }
    }
}

/// ある時点の field の様子（確かめ用）
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldStatus {
    pub snapshot: Snapshot,
    /// 入っている人の数
    pub visitors: usize,
}

enum Command {
    Join {
        reply: oneshot::Sender<Result<Presence, FieldError>>,
    },
    Leave {
        visitor: VisitorId,
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
    /// 入った時点の状態
    snapshot: Snapshot,
    /// 以後の tick。持っているのは最新の 1 枚だけ
    ticks: watch::Receiver<Snapshot>,
    commands: mpsc::UnboundedSender<Command>,
}

impl Presence {
    pub fn visitor(&self) -> VisitorId {
        self.visitor
    }

    /// 入った時点の状態
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// 次の tick を待つ。途中の tick は飛ばし、最新のものを返す。
    /// field が止まったら `None`
    pub async fn next_tick(&mut self) -> Option<Snapshot> {
        self.ticks.changed().await.ok()?;
        Some(self.ticks.borrow_and_update().clone())
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

    /// 入る
    pub async fn join(&self) -> Result<Presence, FieldError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Join { reply })
            .map_err(|_| FieldError::Stopped)?;
        answer.await.map_err(|_| FieldError::Stopped)?
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
pub fn spawn_field(mut field: Field, config: FieldConfig) -> FieldHandle {
    let key = field.key().clone();
    let (commands, mut inbox) = mpsc::unbounded_channel::<Command>();
    let (notes, _) = watch::channel(field.snapshot());
    let presence_commands = commands.downgrade();

    tokio::spawn(async move {
        let name = field.key().clone();
        let mut next_visitor: u64 = 1;
        // 最初の tick は 1 間隔ぶん待ってから
        let mut clock = tokio::time::interval_at(
            tokio::time::Instant::now() + config.tick_interval,
            config.tick_interval,
        );
        clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tracing::info!("field {name}: 動き始めた（{}代め）", field.life());

        loop {
            tokio::select! {
                command = inbox.recv() => match command {
                    Some(Command::Join { reply }) => {
                        let visitor = VisitorId(next_visitor);
                        next_visitor += 1;
                        // 口がもう無いなら、field は畳まれる途中
                        let Some(commands) = presence_commands.upgrade() else {
                            let _ = reply.send(Err(FieldError::Stopped));
                            continue;
                        };
                        let answer = field.join(visitor).map(|snapshot| Presence {
                            visitor,
                            snapshot,
                            // subscribe した時点の値は「見たもの」として扱われる。
                            // 以後に届くのは、この snapshot より後の tick だけ
                            ticks: notes.subscribe(),
                            commands,
                        });
                        // 受け取る側がもういなければ、Presence はここで手放され、
                        // その drop が「出ていく」を送ってくる
                        let _ = reply.send(answer);
                    }
                    Some(Command::Leave { visitor }) => {
                        let _ = field.leave(visitor);
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
                },
                _ = clock.tick() => {
                    if let Ok(note) = field.tick() {
                        // 入っている人がいなくても時計は進む
                        notes.send_replace(note);
                    }
                }
            }
        }
        tracing::info!("field {name}: 止まった");
        // ここで notes が落ち、入っていた人の next_tick が None を返す
    });

    FieldHandle { key, commands }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Place;

    const INTERVAL: Duration = Duration::from_millis(100);

    fn spawn() -> FieldHandle {
        spawn_field(
            Field::new(FieldKey::new("mako", "studio"), Place::Local, 1),
            FieldConfig {
                tick_interval: INTERVAL,
            },
        )
    }

    /// n 回ぶんの tick が過ぎるのを待つ（時間は止めてあるので一瞬で進む）
    async fn pass(n: u32) {
        tokio::time::sleep(INTERVAL * n + INTERVAL / 2).await;
    }

    #[tokio::test(start_paused = true)]
    async fn tick_advances_with_nobody() {
        let field = spawn();
        pass(3).await;
        let status = field.status().await.unwrap();
        assert_eq!(status.snapshot.tick, 3);
        assert_eq!(status.visitors, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn join_gets_snapshot_then_ticks() {
        let field = spawn();
        pass(2).await;
        let mut presence = field.join().await.unwrap();
        assert_eq!(presence.snapshot().tick, 2);
        assert_eq!(presence.next_tick().await.unwrap().tick, 3);
        assert_eq!(presence.next_tick().await.unwrap().tick, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_visitor_skips_to_recent() {
        let field = spawn();
        let mut presence = field.join().await.unwrap();
        // 受け取らないまま 5 回ぶん過ぎる
        pass(5).await;
        // 溜まった 1〜4 は飛ばして、最新の 5 だけが来る
        assert_eq!(presence.next_tick().await.unwrap().tick, 5);
        // その次は 6（古いものは残っていない）
        assert_eq!(presence.next_tick().await.unwrap().tick, 6);
    }

    #[tokio::test(start_paused = true)]
    async fn ticks_never_go_back_and_never_get_ahead() {
        let field = spawn();
        let mut presence = field.join().await.unwrap();
        let mut seen = presence.snapshot().tick;
        for _ in 0..10 {
            let note = presence.next_tick().await.unwrap();
            assert!(note.tick > seen, "戻った: {} → {}", seen, note.tick);
            seen = note.tick;
            let now = field.status().await.unwrap().snapshot.tick;
            assert!(seen <= now, "先に進んだ: 手元 {seen}、field {now}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn two_visitors_both_get_ticks() {
        let field = spawn();
        let mut a = field.join().await.unwrap();
        let mut b = field.join().await.unwrap();
        assert_ne!(a.visitor(), b.visitor());
        assert_eq!(field.status().await.unwrap().visitors, 2);
        assert_eq!(a.next_tick().await.unwrap().tick, 1);
        assert_eq!(b.next_tick().await.unwrap().tick, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn leave_stops_delivery() {
        let field = spawn();
        let presence = field.join().await.unwrap();
        assert_eq!(field.status().await.unwrap().visitors, 1);
        drop(presence);
        assert_eq!(field.status().await.unwrap().visitors, 0);
        // field は動き続ける
        pass(2).await;
        assert_eq!(field.status().await.unwrap().snapshot.tick, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_drops_everyone() {
        let field = spawn();
        let mut presence = field.join().await.unwrap();
        field.stop().await;
        // 入っていた人には「止まった」が伝わる
        assert_eq!(presence.next_tick().await, None);
        assert!(field.status().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cannot_join_stopped() {
        let field = spawn();
        field.stop().await;
        assert!(matches!(field.join().await, Err(FieldError::Stopped)));
    }

    #[tokio::test(start_paused = true)]
    async fn stops_when_every_handle_is_gone() {
        let field = spawn();
        let mut presence = field.join().await.unwrap();
        drop(field);
        // Presence も口を 1 つ持っているので、まだ動いている
        assert_eq!(presence.next_tick().await.unwrap().tick, 1);
    }
}
