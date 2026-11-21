//! 結合テスト: 本物の QUIC で、サーバに入って tick を受け取る。

use std::sync::Arc;
use std::time::Duration;

use field::client::{FieldClient, JoinError};
use field::core::{FieldKey, Place};
use field::host::Host;
use field::protocol::FieldServer;
use field::runtime::FieldConfig;

const INTERVAL: Duration = Duration::from_millis(20);
const PATIENCE: Duration = Duration::from_secs(5);

fn mako_studio() -> FieldKey {
    FieldKey::new("mako", "studio")
}

async fn start() -> (Arc<Host>, FieldServer) {
    let host = Arc::new(Host::new(FieldConfig {
        tick_interval: INTERVAL,
    }));
    let server = FieldServer::start(host.clone(), "[::1]:0")
        .await
        .expect("サーバが立ち上がらない");
    (host, server)
}

async fn within<T>(what: &str, work: impl Future<Output = T>) -> T {
    tokio::time::timeout(PATIENCE, work)
        .await
        .unwrap_or_else(|_| panic!("時間切れ: {what}"))
}

#[tokio::test]
async fn join_gets_snapshot_then_ticks() {
    let (host, server) = start().await;
    host.create(mako_studio(), Place::Local, 7).unwrap();

    let mut visit = within(
        "入る",
        FieldClient::join(server.local_addr(), &mako_studio()),
    )
    .await
    .unwrap();
    let snapshot = visit.snapshot().clone();
    assert_eq!(snapshot.owner, "mako");
    assert_eq!(snapshot.name, "studio");
    assert_eq!(snapshot.place, Place::Local);
    assert_eq!(snapshot.life, 7);

    // 以後の tick は、入った時点より後のものが、順番どおりに届く
    let mut seen = snapshot.tick;
    for _ in 0..5 {
        let note = within("tick", visit.next_tick())
            .await
            .expect("field が止まった");
        assert_eq!(note.life, 7);
        assert!(note.tick > seen, "戻った: {seen} → {}", note.tick);
        seen = note.tick;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn join_missing_field_fails() {
    let (_host, server) = start().await;
    let refused = within(
        "入る",
        FieldClient::join(server.local_addr(), &mako_studio()),
    )
    .await;
    assert!(
        matches!(&refused, Err(JoinError::Refused(reason)) if reason == "no_such_field"),
        "{:?}",
        refused.err()
    );
    server.shutdown().await;
}

#[tokio::test]
async fn leave_stops_delivery() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();

    let visit = within(
        "入る",
        FieldClient::join(server.local_addr(), &mako_studio()),
    )
    .await
    .unwrap();
    assert_eq!(field.status().await.unwrap().visitors, 1);

    // channel を閉じる = 出ていく
    visit.leave().await;
    within("出ていったことが field に伝わる", async {
        while field.status().await.unwrap().visitors != 0 {
            tokio::time::sleep(INTERVAL).await;
        }
    })
    .await;

    // field は動き続ける
    let before = field.status().await.unwrap().snapshot.tick;
    tokio::time::sleep(INTERVAL * 3).await;
    assert!(field.status().await.unwrap().snapshot.tick > before);

    server.shutdown().await;
}

#[tokio::test]
async fn stop_drops_everyone() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();

    let mut visit = within(
        "入る",
        FieldClient::join(server.local_addr(), &mako_studio()),
    )
    .await
    .unwrap();
    field.stop().await;

    // 入っていた人には「止まった」が伝わる
    within("止まったことが伝わる", async {
        while visit.next_tick().await.is_some() {}
    })
    .await;

    // 止まった field には入れない
    let refused = within(
        "入る",
        FieldClient::join(server.local_addr(), &mako_studio()),
    )
    .await;
    assert!(
        matches!(&refused, Err(JoinError::Refused(reason)) if reason == "stopped"),
        "{:?}",
        refused.err()
    );

    server.shutdown().await;
}

#[tokio::test]
async fn fields_are_independent() {
    let (host, server) = start().await;
    let studio = host.create(mako_studio(), Place::Local, 1).unwrap();
    host.create(FieldKey::new("waki", "studio"), Place::Cloud, 2)
        .unwrap();

    let mut at_mako = within(
        "入る",
        FieldClient::join(server.local_addr(), &mako_studio()),
    )
    .await
    .unwrap();
    let mut at_waki = within(
        "入る",
        FieldClient::join(server.local_addr(), &FieldKey::new("waki", "studio")),
    )
    .await
    .unwrap();

    let note = within("tick", at_waki.next_tick()).await.unwrap();
    assert_eq!(
        (note.owner.as_str(), note.place, note.life),
        ("waki", Place::Cloud, 2)
    );
    let note = within("tick", at_mako.next_tick()).await.unwrap();
    assert_eq!(
        (note.owner.as_str(), note.place, note.life),
        ("mako", Place::Local, 1)
    );

    // mako の field を止めても、waki の field には届き続ける
    studio.stop().await;
    within("止まったことが伝わる", async {
        while at_mako.next_tick().await.is_some() {}
    })
    .await;
    assert!(within("tick", at_waki.next_tick()).await.is_some());

    server.shutdown().await;
}

/// 受け取りが遅い人の様子を観察する。
///
/// 約束しているのは「戻らない」「追いつく」まで。途中がどれだけ飛ばされるかは
/// 約束していない（stream に書き込めるうちは飛ばされない）ので、数は表示するだけ。
#[tokio::test]
async fn slow_visitor_never_goes_back_and_catches_up() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();

    let mut visit = within(
        "入る",
        FieldClient::join(server.local_addr(), &mako_studio()),
    )
    .await
    .unwrap();
    let joined_at = visit.snapshot().tick;

    // 受け取らないまま 25 回ぶん待つ
    tokio::time::sleep(INTERVAL * 25).await;
    let target = field.status().await.unwrap().snapshot.tick;

    let mut seen = joined_at;
    let mut received = 0u64;
    while seen < target {
        let note = within("tick", visit.next_tick())
            .await
            .expect("field が止まった");
        assert!(note.tick > seen, "戻った: {seen} → {}", note.tick);
        seen = note.tick;
        received += 1;
    }
    eprintln!(
        "観察: 待っている間に進んだ tick = {}、追いつくまでに受け取った知らせ = {received}",
        target - joined_at
    );

    server.shutdown().await;
}
