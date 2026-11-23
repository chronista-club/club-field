//! 結合テスト: 本物の QUIC で、field に入り、手元の Slave が自動で同期することを確かめる。
//! 後半は、QUIC で下流を待ち受ける中継（`Origin ──QUIC──▶ Relay ──QUIC──▶ Leaf`）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use field::client::{FieldSlave, JoinError};
use field::core::{FieldKey, FieldSnapshot, Place};
use field::host::Host;
use field::protocol::{
    FEED, FieldServer, JOIN, JoinReply, JoinRequest, NAMESPACE, PRESENCE, PROTOCOL_NAME, SET,
    SetReply, SetRequest,
};
use field::relay::RelayHandle;
use field::sync::{FeedAcceptor, Slave, SlotState, SlotUpdate, SyncError, proto};

const PATIENCE: Duration = Duration::from_secs(5);

fn mako_studio() -> FieldKey {
    FieldKey::new("mako", "studio")
}

fn state(rev: u64, value: Value) -> SlotState {
    SlotState::from_json(rev, &value)
}

async fn start() -> (Arc<Host>, FieldServer) {
    let host = Arc::new(Host::new());
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

async fn join(server: &FieldServer, key: &FieldKey) -> FieldSlave {
    within("入る", FieldSlave::join(server.local_addr(), key))
        .await
        .expect("入れない")
}

/// 手元の Slave が条件を満たすまで、変動を待つ
async fn until(slave: &mut Slave, what: &str, done: impl Fn(&Slave) -> bool) {
    within(what, async {
        while !done(slave) {
            if slave.changed().await.is_none() {
                panic!("外れた: {what}");
            }
        }
    })
    .await;
}

#[tokio::test]
async fn join_gets_snapshot() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 7).unwrap();
    field.set("light", json!(0.8)).await.unwrap();

    let slave = join(&server, &mako_studio()).await;
    let info = slave.info();
    assert_eq!(
        (
            info.owner.as_str(),
            info.name.as_str(),
            info.place,
            info.life
        ),
        ("mako", "studio", Place::Local, 7)
    );
    // 入った時点の状態は、待たずに手元にある
    assert_eq!(slave.get("light"), Some(state(1, json!(0.8))));
    assert_eq!(slave.get("camera"), None);

    server.shutdown().await;
}

#[tokio::test]
async fn origin_set_reaches_slave() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let mut slave = join(&server, &mako_studio()).await;

    field.set("camera", json!("wide")).await.unwrap();
    assert_eq!(
        within("変動", slave.changed()).await,
        Some(vec![SlotUpdate::from_json(1, "camera", 1, &json!("wide"))])
    );
    assert_eq!(slave.get("camera"), Some(state(1, json!("wide"))));

    server.shutdown().await;
}

#[tokio::test]
async fn slave_set_is_confirmed_by_origin_and_reaches_others() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let writer = join(&server, &mako_studio()).await;
    let mut other = join(&server, &mako_studio()).await;

    let confirmed = within("頼む", writer.set("light", json!(2))).await.unwrap();
    assert_eq!((confirmed.rev, confirmed.value()), (1, json!(2)));
    // 頼んだ側は、変動の知らせを待たずに確定した値を持つ
    assert_eq!(writer.get("light"), Some(state(1, json!(2))));
    // 正本（Origin）で確定している
    assert_eq!(
        field.status().await.unwrap().snapshot.slots.get("light"),
        Some(&state(1, json!(2)))
    );
    // ほかの Slave にも自動で届く
    until(&mut other, "ほかの Slave に届く", |slave| {
        slave.get("light") == Some(state(1, json!(2)))
    })
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn slots_merge_from_many_writers() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let mut a = join(&server, &mako_studio()).await;
    let mut b = join(&server, &mako_studio()).await;
    let mut watcher = join(&server, &mako_studio()).await;

    // 3 人（持ち主と、入っている 2 人）が、別々の要素と同じ要素を、入り混じって置き換える
    let by_owner = async {
        for i in 0..30 {
            field.set("light", json!(i)).await.unwrap();
        }
    };
    let by_a = async {
        for i in 0..30 {
            a.set("camera", json!(i)).await.unwrap();
        }
    };
    let by_b = async {
        for i in 0..30 {
            b.set("light", json!(-i)).await.unwrap();
            b.set("sound", json!(i)).await.unwrap();
        }
    };
    within("置き換える", async {
        tokio::join!(by_owner, by_a, by_b)
    })
    .await;

    // 落ち着けば、全員の手元が正本と同じになる
    let origin = field.status().await.unwrap().snapshot.slots;
    assert_eq!(origin["light"].rev, 60);
    assert_eq!(origin["camera"].rev, 30);
    assert_eq!(origin["sound"].rev, 30);
    for (who, slave) in [("a", &mut a), ("b", &mut b), ("watcher", &mut watcher)] {
        until(slave, who, |slave| slave.snapshot().slots == origin).await;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn slow_consumer_slave_is_always_current() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let mut slave = join(&server, &mako_studio()).await;

    // 手元の利用者は changed() を呼ばないまま、200 回置き換わる
    for i in 1..=200 {
        field.set("light", json!(i)).await.unwrap();
    }
    // それでも Slave は裏で吸い上げ続け、最新になっている
    within("Slave が追いつく", async {
        while slave.get("light") != Some(state(200, json!(200))) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    // 利用者が見に来たときには、畳まれた最新の 1 つだけ
    assert_eq!(
        within("変動", slave.changed()).await,
        Some(vec![SlotUpdate::from_json(1, "light", 200, &json!(200))])
    );

    server.shutdown().await;
}

/// 中継: QUIC で入った Slave を、手元の別の Slave の配り手にする
#[tokio::test]
async fn relay_over_a_quic_slave() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let relay = join(&server, &mako_studio()).await;
    let mut downstream = Slave::follow(&*relay);

    // Origin の変動は、中継を経て下流に届く。rev は Origin が振ったまま
    let update = field.set("light", json!(1)).await.unwrap();
    until(&mut downstream, "下流に届く", |slave| {
        slave.get("light") == Some(state(update.rev, json!(1)))
    })
    .await;

    // 下流からの頼みは、中継を経て Origin で確定する
    let confirmed = within("下流から頼む", downstream.set("camera", json!("wide")))
        .await
        .unwrap();
    assert_eq!(
        field.status().await.unwrap().snapshot.slots.get("camera"),
        Some(&state(confirmed.rev, json!("wide")))
    );
    assert_eq!(
        relay.get("camera"),
        Some(state(confirmed.rev, json!("wide")))
    );
    assert_eq!(
        downstream.get("camera"),
        Some(state(confirmed.rev, json!("wide")))
    );

    // 中継が出ていくと、下流も外れる
    relay.leave().await;
    within("下流も外れる", async {
        while downstream.changed().await.is_some() {}
    })
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn join_missing_field_fails() {
    let (_host, server) = start().await;
    let refused = within(
        "入る",
        FieldSlave::join(server.local_addr(), &mako_studio()),
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

    let slave = join(&server, &mako_studio()).await;
    assert_eq!(field.status().await.unwrap().visitors, 1);

    // 手放す = 出ていく
    slave.leave().await;
    within("出ていったことが field に伝わる", async {
        while field.status().await.unwrap().visitors != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;

    // field は動き続ける
    assert!(field.set("light", json!(1)).await.is_ok());

    server.shutdown().await;
}

#[tokio::test]
async fn stop_drops_everyone() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();

    let mut slave = join(&server, &mako_studio()).await;
    field.stop().await;

    // 入っていた人には「止まった」が伝わる
    within("止まったことが伝わる", async {
        while slave.changed().await.is_some() {}
    })
    .await;
    assert!(slave.is_closed());
    assert_eq!(slave.set("light", json!(1)).await, Err(SyncError::Closed));

    // 止まった field には入れない
    let refused = within(
        "入る",
        FieldSlave::join(server.local_addr(), &mako_studio()),
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
    let waki_key = FieldKey::new("waki", "studio");
    let waki = host.create(waki_key.clone(), Place::Cloud, 2).unwrap();

    let mut at_mako = join(&server, &mako_studio()).await;
    let mut at_waki = join(&server, &waki_key).await;
    assert_eq!(
        (at_waki.info().place, at_waki.info().life),
        (Place::Cloud, 2)
    );

    waki.set("light", json!(1)).await.unwrap();
    until(&mut at_waki, "waki に届く", |slave| {
        slave.get("light").is_some()
    })
    .await;
    assert_eq!(at_mako.get("light"), None);

    // mako の field を止めても、waki の field には届き続ける
    studio.stop().await;
    within("止まったことが伝わる", async {
        while at_mako.changed().await.is_some() {}
    })
    .await;
    waki.set("light", json!(2)).await.unwrap();
    until(&mut at_waki, "waki に届き続ける", |slave| {
        slave.get("light") == Some(state(2, json!(2)))
    })
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn bad_set_is_refused() {
    let (host, server) = start().await;
    host.create(mako_studio(), Place::Local, 1).unwrap();
    let slave = join(&server, &mako_studio()).await;
    assert_eq!(
        slave.set("", json!(1)).await,
        Err(SyncError::Refused("bad_request".into()))
    );
    server.shutdown().await;
}

/// プロトコルの決まり: 入る前に置き換えは頼めない
#[tokio::test]
async fn set_without_join_is_refused() {
    let (host, server) = start().await;
    host.create(mako_studio(), Place::Local, 1).unwrap();

    let client = unison::ProtocolClient::insecure_localhost().unwrap();
    within("つなぐ", client.connect(&server.local_addr().to_string()))
        .await
        .unwrap();
    let channel = within("開く", client.open_channel(PRESENCE)).await.unwrap();
    let reply: SetReply = within(
        "頼む",
        channel.request(SET, &SetRequest::from_json("light", &json!(1))),
    )
    .await
    .unwrap();
    assert!(
        matches!(&reply, SetReply::Refused { error } if error == "not_joined"),
        "{reply:?}"
    );

    // 同じ channel で 2 度は入れない
    let first: JoinReply = within(
        "入る",
        channel.request(
            JOIN,
            &JoinRequest {
                owner: "mako".into(),
                name: "studio".into(),
            },
        ),
    )
    .await
    .unwrap();
    assert!(matches!(first, JoinReply::Joined(_)), "{first:?}");
    let second: JoinReply = within(
        "もう一度入る",
        channel.request(
            JOIN,
            &JoinRequest {
                owner: "mako".into(),
                name: "studio".into(),
            },
        ),
    )
    .await
    .unwrap();
    assert!(
        matches!(&second, JoinReply::Refused { error } if error == "already_joined"),
        "{second:?}"
    );

    let _ = client.disconnect().await;
    server.shutdown().await;
}

/// 1 つの Slave から（たとえば中継が下流の頼みをまとめて）大量の頼みを一度に出しても、
/// 黙って捨てられずに全部確定する
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_sets_are_all_confirmed() {
    const SETS: u64 = 3000;
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let slave = Arc::new(join(&server, &mako_studio()).await);

    let mut sets = tokio::task::JoinSet::new();
    for i in 0..SETS {
        let slave = slave.clone();
        sets.spawn(async move { slave.set("light", json!(i)).await });
    }
    let confirmed = tokio::time::timeout(Duration::from_secs(20), sets.join_all())
        .await
        .expect("時間切れ: 頼みが捨てられた");
    assert!(
        confirmed.iter().all(Result::is_ok),
        "確定しなかった頼みがある"
    );
    assert_eq!(
        field.status().await.unwrap().snapshot.slots["light"].rev,
        SETS
    );

    server.shutdown().await;
}

/// feed を受け取らないクライアントは、最初の変動を書こうとした時点で外される。
/// 入ったまま置き換えを頼み続けることはできない
#[tokio::test]
async fn client_without_feed_is_dropped() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();

    // feed の受け口を置かずに入る
    let client = unison::ProtocolClient::insecure_localhost().unwrap();
    within("つなぐ", client.connect(&server.local_addr().to_string()))
        .await
        .unwrap();
    let channel = within("開く", client.open_channel(PRESENCE)).await.unwrap();
    let reply: JoinReply = within(
        "入る",
        channel.request(
            JOIN,
            &JoinRequest {
                owner: "mako".into(),
                name: "studio".into(),
            },
        ),
    )
    .await
    .unwrap();
    assert!(matches!(reply, JoinReply::Joined(_)), "{reply:?}");

    // 持ち主が置き換え続けると、feed に書けなくなった時点で外される
    within("外される", async {
        let mut i = 0;
        while field.status().await.unwrap().visitors != 0 {
            field.set("light", json!(i)).await.unwrap();
            i += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    // presence も閉じているので、頼みは届かない
    let after: Result<SetReply, _> = within(
        "頼む",
        channel.request(SET, &SetRequest::from_json("light", &json!(-1))),
    )
    .await;
    assert!(after.is_err(), "{after:?}");

    let _ = client.disconnect().await;
    server.shutdown().await;
}

/// 入るのに途中で失敗しても（ここでは presence を持たないサーバ）、つながりは残らない
#[tokio::test]
async fn failed_join_leaves_no_connection() {
    let server = Arc::new(unison::ProtocolServer::with_identity(
        "not-field",
        "0.0.0",
        "example.not_field",
    ));
    let handle = server
        .clone()
        .listener("[::1]:0")
        .spawn()
        .await
        .expect("サーバが立ち上がらない");

    let failed = within(
        "入る",
        FieldSlave::join(handle.local_addr(), &mako_studio()),
    )
    .await;
    assert!(
        matches!(failed, Err(JoinError::Network(_))),
        "{:?}",
        failed.err()
    );
    within("つながりが閉じる", async {
        while server.active_connection_count().await != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;

    let _ = handle.shutdown().await;
}

// ── 中継: QUIC で下流を待ち受ける ─────────────────────────────

/// 上流（`upstream` で待ち受けている field、または中継）に入り、中継として下流を待ち受ける
async fn start_relay(upstream: &FieldServer, key: &FieldKey) -> (RelayHandle, FieldServer) {
    let slave = join(upstream, key).await;
    let host = Arc::new(Host::new());
    let relay = host.relay(slave).expect("中継を置けない");
    let server = FieldServer::start(host, "[::1]:0")
        .await
        .expect("中継が待ち受けられない");
    (relay, server)
}

/// 中継に入ろうとして断られた理由
async fn refused_reason(server: &FieldServer, key: &FieldKey) -> String {
    match within("入る", FieldSlave::join(server.local_addr(), key)).await {
        Err(JoinError::Refused(reason)) => reason,
        Err(other) => panic!("断られずに失敗した: {other}"),
        Ok(_) => panic!("入れてしまった"),
    }
}

/// 手元の Slave に「止まった」が伝わるまで待つ
async fn until_dropped(slave: &mut Slave, what: &str) {
    within(what, async { while slave.changed().await.is_some() {} }).await;
    assert!(slave.is_closed(), "{what}");
}

/// field の側で、入っている人の数が `count` になるまで待つ
async fn until_visitors(field: &field::runtime::FieldHandle, count: usize) {
    within("入っている人の数", async {
        while field.status().await.unwrap().visitors != count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn relay_delivers_origin_updates_downstream() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Cloud, 7).unwrap();
    field.set("light", json!(0.8)).await.unwrap();
    let (relay, relay_server) = start_relay(&origin_server, &mako_studio()).await;

    let mut leaf = join(&relay_server, &mako_studio()).await;
    // 中継を通して入っても、名札と代は field のもの。入った時点の状態は待たずに手元にある
    assert_eq!(
        (
            leaf.info().owner.as_str(),
            leaf.info().place,
            leaf.info().life
        ),
        ("mako", Place::Cloud, 7)
    );
    assert_eq!(leaf.get("light"), Some(state(1, json!(0.8))));
    assert_eq!(relay.visitors(), 1);
    // Origin から見ると、入っているのは中継 1 人だけ
    assert_eq!(field.status().await.unwrap().visitors, 1);

    // Origin の変動は、中継を経て自動で届く。rev は Origin が振ったまま
    field.set("light", json!(0.5)).await.unwrap();
    field.set("camera", json!("wide")).await.unwrap();
    let origin = field.status().await.unwrap().snapshot.slots;
    until(&mut leaf, "中継を経て届く", |slave| {
        slave.snapshot().slots == origin
    })
    .await;
    assert_eq!(leaf.get("light"), Some(state(2, json!(0.5))));

    // 中継に無い field には入れない
    assert_eq!(
        refused_reason(&relay_server, &FieldKey::new("waki", "studio")).await,
        "no_such_field"
    );

    relay_server.shutdown().await;
    origin_server.shutdown().await;
}

#[tokio::test]
async fn leaf_set_through_relay_reaches_everyone() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let (relay, relay_server) = start_relay(&origin_server, &mako_studio()).await;
    let writer = join(&relay_server, &mako_studio()).await;
    let mut other_leaf = join(&relay_server, &mako_studio()).await;
    let mut direct = join(&origin_server, &mako_studio()).await;

    let confirmed = within("中継を経て頼む", writer.set("light", json!(2)))
        .await
        .unwrap();
    assert_eq!((confirmed.rev, confirmed.value()), (1, json!(2)));
    // Origin で確定し、応答の道で中継にも頼んだ人にも合流している
    assert_eq!(
        field.status().await.unwrap().snapshot.slots.get("light"),
        Some(&state(1, json!(2)))
    );
    assert_eq!(
        relay.snapshot().unwrap().slots.get("light"),
        Some(&state(1, json!(2)))
    );
    assert_eq!(writer.get("light"), Some(state(1, json!(2))));
    // 中継を通して入っている別の人にも、Origin に直接入っている人にも届く
    for (who, slave) in [
        ("別の Leaf", &mut other_leaf),
        ("直結の Slave", &mut direct),
    ] {
        until(slave, who, |slave| {
            slave.get("light") == Some(state(1, json!(2)))
        })
        .await;
    }

    // 空の要素名は、中継が上流へ運ぶ前に断る
    assert_eq!(
        writer.set("", json!(1)).await,
        Err(SyncError::Refused("bad_request".into()))
    );

    relay_server.shutdown().await;
    origin_server.shutdown().await;
}

#[tokio::test]
async fn concurrent_leaf_sets_converge() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let (relay, relay_server) = start_relay(&origin_server, &mako_studio()).await;
    let mut a = join(&relay_server, &mako_studio()).await;
    let mut b = join(&relay_server, &mako_studio()).await;
    let mut direct = join(&origin_server, &mako_studio()).await;

    // 持ち主・中継を通した 2 人・直結の 1 人が、同じ要素と別々の要素を、入り混じって置き換える
    let by_owner = async {
        for i in 0..30 {
            field.set("light", json!(i)).await.unwrap();
        }
    };
    let by_a = async {
        for i in 0..30 {
            a.set("light", json!(-i)).await.unwrap();
            a.set("camera", json!(i)).await.unwrap();
        }
    };
    let by_b = async {
        for i in 0..30 {
            b.set("light", json!(100 + i)).await.unwrap();
        }
    };
    let by_direct = async {
        for i in 0..30 {
            direct.set("sound", json!(i)).await.unwrap();
        }
    };
    within("置き換える", async {
        tokio::join!(by_owner, by_a, by_b, by_direct)
    })
    .await;

    // 落ち着けば、全員の手元が Origin の確定値と同じになる
    let origin = field.status().await.unwrap().snapshot.slots;
    assert_eq!(origin["light"].rev, 90);
    assert_eq!(origin["camera"].rev, 30);
    assert_eq!(origin["sound"].rev, 30);
    for (who, slave) in [("a", &mut a), ("b", &mut b), ("直結", &mut direct)] {
        until(slave, who, |slave| slave.snapshot().slots == origin).await;
    }
    within("中継が揃う", async {
        while relay.snapshot().unwrap().slots != origin {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;

    relay_server.shutdown().await;
    origin_server.shutdown().await;
}

#[tokio::test]
async fn leaf_leaving_keeps_others_running() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let (relay, relay_server) = start_relay(&origin_server, &mako_studio()).await;
    let leaving = join(&relay_server, &mako_studio()).await;
    let mut staying = join(&relay_server, &mako_studio()).await;
    assert_eq!(relay.visitors(), 2);

    leaving.leave().await;
    within("出ていったことが中継に伝わる", async {
        while relay.visitors() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;

    // 中継も Origin も、残った Leaf も動き続ける
    assert!(!relay.is_stopped());
    assert_eq!(field.status().await.unwrap().visitors, 1);
    field.set("light", json!(1)).await.unwrap();
    until(&mut staying, "残った Leaf に届く", |slave| {
        slave.get("light") == Some(state(1, json!(1)))
    })
    .await;
    assert!(staying.set("camera", json!(2)).await.is_ok());

    relay_server.shutdown().await;
    origin_server.shutdown().await;
}

#[tokio::test]
async fn relay_stop_drops_only_its_leaves() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let (relay, relay_server) = start_relay(&origin_server, &mako_studio()).await;
    let mut leaf = join(&relay_server, &mako_studio()).await;
    let mut direct = join(&origin_server, &mako_studio()).await;
    until_visitors(&field, 2).await;

    relay.stop().await;
    // 中継の下流には「止まった」が伝わる
    until_dropped(&mut leaf, "Leaf が外れる").await;
    assert_eq!(leaf.set("light", json!(1)).await, Err(SyncError::Closed));
    // 中継は上流から出ていった。Origin と直結の Slave は動き続ける
    until_visitors(&field, 1).await;
    field.set("light", json!(1)).await.unwrap();
    until(&mut direct, "直結の Slave に届く", |slave| {
        slave.get("light") == Some(state(1, json!(1)))
    })
    .await;
    // 止まった中継には入れない
    assert_eq!(
        refused_reason(&relay_server, &mako_studio()).await,
        "stopped"
    );

    relay_server.shutdown().await;
    origin_server.shutdown().await;
}

#[tokio::test]
async fn origin_stop_reaches_leaves_through_relay() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let (relay, relay_server) = start_relay(&origin_server, &mako_studio()).await;
    let mut leaf = join(&relay_server, &mako_studio()).await;
    let mut watching = relay.observe().unwrap();

    field.stop().await;
    // 中継にも、中継の下流にも「止まった」が伝わる
    within("中継が外れる", async {
        while watching.next().await.is_some() {}
    })
    .await;
    assert!(relay.is_stopped());
    until_dropped(&mut leaf, "Leaf が外れる").await;
    // 止まった中継には入れない
    assert_eq!(
        refused_reason(&relay_server, &mako_studio()).await,
        "stopped"
    );

    relay.stop().await;
    relay_server.shutdown().await;
    origin_server.shutdown().await;
}

/// `Origin ──QUIC──▶ Relay 1 ──QUIC──▶ Relay 2 ──QUIC──▶ Leaf`
#[tokio::test]
async fn relay_chain_over_quic() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let (first, first_server) = start_relay(&origin_server, &mako_studio()).await;
    let (second, second_server) = start_relay(&first_server, &mako_studio()).await;
    let mut leaf = join(&second_server, &mako_studio()).await;

    // 下りの道: Origin の変動は 2 段の中継を経て届く
    field.set("light", json!(1)).await.unwrap();
    until(&mut leaf, "3 段めに届く", |slave| {
        slave.get("light") == Some(state(1, json!(1)))
    })
    .await;

    // 上りの道: Leaf の頼みは 2 段の中継を経て Origin で確定し、各段に合流する
    let confirmed = within("3 段めから頼む", leaf.set("camera", json!("wide")))
        .await
        .unwrap();
    assert_eq!(confirmed.rev, 1);
    assert_eq!(
        field.status().await.unwrap().snapshot.slots.get("camera"),
        Some(&state(1, json!("wide")))
    );
    for (who, relay) in [("Relay 1", &first), ("Relay 2", &second)] {
        assert_eq!(
            relay.snapshot().unwrap().slots.get("camera"),
            Some(&state(1, json!("wide"))),
            "{who}"
        );
    }
    assert_eq!(leaf.get("camera"), Some(state(1, json!("wide"))));

    // 真ん中の中継が止まると、その先は外れる。手前の中継と Origin は動き続ける
    second.stop().await;
    until_dropped(&mut leaf, "Leaf が外れる").await;
    assert!(!first.is_stopped());
    assert!(first.set("light", json!(2)).await.is_ok());

    second_server.shutdown().await;
    first.stop().await;
    first_server.shutdown().await;
    origin_server.shutdown().await;
}

/// 偽の上流（field の代わり）。`slow` という要素の頼みにだけ返事をせず、届いたことを知らせる。
/// それ以外の頼みは、空の要素名でも何でも確定させる
async fn start_stalling_upstream() -> (
    unison::network::server::ServerHandle,
    Arc<tokio::sync::Notify>,
) {
    use unison::network::{MessageType, UnisonChannel};

    let heard_slow = Arc::new(tokio::sync::Notify::new());
    let server = unison::ProtocolServer::with_identity(PROTOCOL_NAME, "0.0.0", NAMESPACE);
    let heard = heard_slow.clone();
    server
        .register_channel(PRESENCE, move |ctx, stream| {
            let heard = heard.clone();
            async move {
                let channel = UnisonChannel::new(stream);
                let mut rev = 0;
                // feed は開いたまま持っておく（何も書かない）
                let mut _feed = None;
                loop {
                    let Ok(message) = channel.recv().await else {
                        return Ok(());
                    };
                    if message.msg_type != MessageType::Request {
                        continue;
                    }
                    match message.method.as_str() {
                        JOIN => {
                            _feed = Some(ctx.open_server_stream(FEED).await?);
                            let snapshot = FieldSnapshot {
                                owner: "mako".into(),
                                name: "studio".into(),
                                place: Place::Local,
                                life: 1,
                                slots: BTreeMap::new(),
                            };
                            channel
                                .send_response(message.id, JOIN, &JoinReply::Joined(snapshot))
                                .await?;
                        }
                        SET => {
                            let request: SetRequest =
                                serde_json::from_value(message.payload_as_value()?).unwrap();
                            if request.slot == "slow" {
                                heard.notify_one();
                                continue;
                            }
                            rev += 1;
                            let update =
                                SlotUpdate::from_bytes(1, request.slot, rev, request.payload);
                            channel
                                .send_response(message.id, SET, &SetReply::Set(update))
                                .await?;
                        }
                        _ => {}
                    }
                }
            }
        })
        .await;
    let handle = Arc::new(server)
        .listener("[::1]:0")
        .spawn()
        .await
        .expect("偽の上流が立ち上がらない");
    (handle, heard_slow)
}

/// 中継が、ある Leaf の頼みで上流の返事を待っていても、ほかの Leaf は入れて、頼める
#[tokio::test]
async fn relay_waiting_upstream_holds_up_no_other_leaf() {
    let (upstream, heard_slow) = start_stalling_upstream().await;
    let slave = within(
        "上流に入る",
        FieldSlave::join(upstream.local_addr(), &mako_studio()),
    )
    .await
    .unwrap();
    let host = Arc::new(Host::new());
    let relay = host.relay(slave).unwrap();
    let relay_server = FieldServer::start(host, "[::1]:0").await.unwrap();

    let stuck = Arc::new(join(&relay_server, &mako_studio()).await);
    let slow = tokio::spawn({
        let stuck = stuck.clone();
        async move { stuck.set("slow", json!(1)).await }
    });
    // 頼みが中継を経て上流に届き、中継が上流の返事を待ち始めるまで
    within("頼みが上流に届く", heard_slow.notified()).await;
    assert!(!slow.is_finished());

    let other = join(&relay_server, &mako_studio()).await;
    let fast = within("ほかの Leaf の頼み", other.set("fast", json!(2)))
        .await
        .unwrap();
    assert_eq!((fast.rev, fast.value()), (1, json!(2)));
    assert!(!slow.is_finished());
    // 偽の上流は空の要素名でも確定させる。断られるのは、中継が上流へ運ぶ前に断るから
    assert_eq!(
        other.set("", json!(1)).await,
        Err(SyncError::Refused("bad_request".into()))
    );

    // 中継が止まると、返事を待っていた頼みも終わる（上流の時間切れを待たない）
    relay.stop().await;
    let slow = within("待っていた頼みが終わる", slow).await.unwrap();
    assert!(slow.is_err(), "{slow:?}");

    relay_server.shutdown().await;
    let _ = upstream.shutdown().await;
}

// ── 値のバイト列 ─────────────────────────────────────────

/// JSON でないバイト列（protobuf の varint の形）
const RAW: &[u8] = &[0x08, 0x96, 0x01];

/// protobuf の値の例。unison の wire の型（SlotEntryProto）を借りる
fn mesh_message() -> proto::SlotEntryProto {
    proto::SlotEntryProto {
        slot: "inner".into(),
        rev: 9,
        payload: vec![1, 2, 3],
        ..Default::default()
    }
}

/// 手元の Slave の要素を、protobuf として写さずに読む
fn read_mesh(slave: &Slave) -> Option<(String, u64, Vec<u8>)> {
    slave.with_slot("mesh", |state| {
        let view = state
            .decode_view::<proto::SlotEntryProtoView>()
            .expect("protobuf として読めない");
        (view.slot.to_string(), view.rev, view.payload.to_vec())
    })
}

/// 持ち主が置き換えたバイト列は、feed（buffa）を通って、直に入った人にも、中継を経た人にも、そのまま届く
#[tokio::test]
async fn bytes_reach_slaves_through_feed_as_is() {
    let (host, origin_server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let (_relay, relay_server) = start_relay(&origin_server, &mako_studio()).await;
    let mut direct = join(&origin_server, &mako_studio()).await;
    let mut leaf = join(&relay_server, &mako_studio()).await;

    let message = SetRequest::from_proto("mesh", &mesh_message());
    field
        .set_bytes("mesh", message.payload.clone())
        .await
        .unwrap();
    field.set_bytes("raw", RAW).await.unwrap();

    for (slave, who) in [(&mut direct, "直に入った人"), (&mut leaf, "中継を経た人")] {
        until(slave, who, |slave| {
            slave.get("mesh").is_some() && slave.get("raw").is_some()
        })
        .await;
        assert_eq!(
            slave.get("mesh"),
            Some(SlotState::from_bytes(1, message.payload.clone())),
            "{who}"
        );
        assert_eq!(
            slave.get("raw"),
            Some(SlotState::from_bytes(1, RAW)),
            "{who}"
        );
        assert_eq!(
            read_mesh(slave),
            Some(("inner".to_string(), 9, vec![1, 2, 3])),
            "{who}"
        );
    }

    relay_server.shutdown().await;
    origin_server.shutdown().await;
}

/// JSON の値で置き換えたもの（`set(Value)` が作る、詰めた JSON）は、presence（JSON の道）を通っても
/// 同じバイト列に戻る。後から入った人の写しも、頼んだ人の応答も、正本とバイト単位で同じ（設計の D18）
#[tokio::test]
async fn json_values_keep_bytes_through_presence() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let value = json!({ "b": 1, "a": [1.5, "x", null], "c": { "z": true, "y": -2 } });
    field.set("scene", value.clone()).await.unwrap();

    // 後から入った人は、Join の応答（JSON）で受け取る
    let late = join(&server, &mako_studio()).await;
    let origin = || async { field.status().await.unwrap().snapshot.slots["scene"].clone() };
    assert_eq!(late.get("scene"), Some(origin().await));

    // 入った人が頼むと、Set（JSON）で運ばれ、応答（JSON）で戻る
    let confirmed = within("頼む", late.set("scene", value.clone()))
        .await
        .unwrap();
    assert_eq!(confirmed.rev, 2);
    assert_eq!(&confirmed.payload, origin().await.bytes());
    assert_eq!(late.get("scene"), Some(origin().await));

    server.shutdown().await;
}

/// presence の `Set` を buffa の形（`SetRequestProto`）で送る。
/// field の presence は JSON の channel なので、組み立て済みのバイト列をそのまま載せる
struct ProtoSet(Vec<u8>);

impl unison::codec::Encodable<unison::codec::JsonCodec> for ProtoSet {
    fn encode(&self) -> Result<Vec<u8>, unison::codec::CodecError> {
        Ok(self.0.clone())
    }
}

/// presence に buffa の形で届いた `Set` の値は、JSON に戻されず、そのバイト列のまま正本になる
#[tokio::test]
async fn proto_set_on_presence_keeps_bytes() {
    let (host, server) = start().await;
    let field = host.create(mako_studio(), Place::Local, 1).unwrap();
    let mut other = join(&server, &mako_studio()).await;

    let client = unison::ProtocolClient::insecure_localhost().unwrap();
    // feed の受け口を置いておく（受け取れないと、field は feed に書けず presence を閉じる）
    let feed = FeedAcceptor::register(&client, FEED).await;
    within("つなぐ", client.connect(&server.local_addr().to_string()))
        .await
        .unwrap();
    let channel = within("開く", client.open_channel(PRESENCE)).await.unwrap();
    let joined: JoinReply = within(
        "入る",
        channel.request(
            JOIN,
            &JoinRequest {
                owner: "mako".into(),
                name: "studio".into(),
            },
        ),
    )
    .await
    .unwrap();
    assert!(matches!(joined, JoinReply::Joined(_)), "{joined:?}");
    let _feed = feed.wait(PATIENCE).await.expect("feed が開かれない");

    let request = SetRequest::from_bytes("raw", RAW);
    let wire =
        <SetRequest as unison::codec::Encodable<unison::codec::ProtoCodec>>::encode(&request)
            .unwrap();
    let reply: SetReply = within("頼む", channel.request(SET, &ProtoSet(wire)))
        .await
        .unwrap();
    assert!(
        matches!(&reply, SetReply::Set(update) if update.rev == 1),
        "{reply:?}"
    );
    // 正本は、送ったバイト列そのもの
    assert_eq!(
        field.status().await.unwrap().snapshot.slots.get("raw"),
        Some(&SlotState::from_bytes(1, RAW))
    );
    // ほかに入っている人にも、feed でそのまま届く
    until(&mut other, "feed", |slave| slave.get("raw").is_some()).await;
    assert_eq!(other.get("raw"), Some(SlotState::from_bytes(1, RAW)));

    let _ = client.disconnect().await;
    server.shutdown().await;
}
