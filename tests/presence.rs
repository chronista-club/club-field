//! 結合テスト: 本物の QUIC で、field に入り、手元の Slave が自動で同期することを確かめる。

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use field::client::{FieldSlave, JoinError};
use field::core::{FieldKey, Place};
use field::host::Host;
use field::protocol::{
    FieldServer, JOIN, JoinReply, JoinRequest, PRESENCE, SET, SetReply, SetRequest,
};
use field::sync::{Slave, SlotState, SlotUpdate, SyncError};

const PATIENCE: Duration = Duration::from_secs(5);

fn mako_studio() -> FieldKey {
    FieldKey::new("mako", "studio")
}

fn state(rev: u64, value: Value) -> SlotState {
    SlotState { rev, value }
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
        Some(vec![SlotUpdate {
            life: 1,
            slot: "camera".into(),
            rev: 1,
            value: json!("wide"),
        }])
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
    assert_eq!((confirmed.rev, &confirmed.value), (1, &json!(2)));
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
        Some(vec![SlotUpdate {
            life: 1,
            slot: "light".into(),
            rev: 200,
            value: json!(200),
        }])
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
        channel.request(
            SET,
            &SetRequest {
                slot: "light".into(),
                value: json!(1),
            },
        ),
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
        channel.request(
            SET,
            &SetRequest {
                slot: "light".into(),
                value: json!(-1),
            },
        ),
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
