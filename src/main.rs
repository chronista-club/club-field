//! `field` コマンド。
//!
//! - `field up`    自分の field を持つ（動かし始める）
//! - `field watch` 動いている field に入って、tick を表示する（確かめ用）

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};

use field::client::FieldClient;
use field::core::{FieldKey, Place};
use field::host::Host;
use field::protocol::FieldServer;
use field::runtime::FieldConfig;

/// 待ち受け先の既定。ポート番号は仮（台帳で正式に取るまで）
const DEFAULT_ADDR: &str = "[::1]:7880";

#[derive(Parser)]
#[command(name = "field", version, about = "Chronista Club の field を動かす")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 自分の field を持つ（動かし始める）。Ctrl-C で止まる
    Up {
        /// 持ち主
        #[arg(long)]
        owner: String,
        /// field の名前（持ち主の中で一意）
        #[arg(long)]
        name: String,
        /// field が住む場所
        #[arg(long, value_enum, default_value_t = PlaceArg::Local)]
        place: PlaceArg,
        /// 待ち受け先。認可がまだ無いので、自分のマシンの中（loopback）だけ
        #[arg(long, default_value = DEFAULT_ADDR)]
        listen: SocketAddr,
        /// 時計の間隔（ミリ秒）
        #[arg(long, default_value_t = 1000)]
        tick_ms: u64,
    },
    /// 動いている field に入って、tick を表示する（確かめ用）
    Watch {
        /// 持ち主
        #[arg(long)]
        owner: String,
        /// field の名前
        #[arg(long)]
        name: String,
        /// つなぐ先
        #[arg(long, default_value = DEFAULT_ADDR)]
        connect: SocketAddr,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum PlaceArg {
    Local,
    Cloud,
}

impl From<PlaceArg> for Place {
    fn from(place: PlaceArg) -> Self {
        match place {
            PlaceArg::Local => Place::Local,
            PlaceArg::Cloud => Place::Cloud,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Command::Up {
            owner,
            name,
            place,
            listen,
            tick_ms,
        } => up(FieldKey::new(owner, name), place.into(), listen, tick_ms).await,
        Command::Watch {
            owner,
            name,
            connect,
        } => watch(FieldKey::new(owner, name), connect).await,
    }
}

async fn up(key: FieldKey, place: Place, listen: SocketAddr, tick_ms: u64) -> anyhow::Result<()> {
    if !listen.ip().is_loopback() {
        bail!(
            "{listen} は自分のマシンの外から届く。認可がまだ無いので、loopback（例: {DEFAULT_ADDR}）だけにしてある"
        );
    }
    if tick_ms == 0 {
        bail!("--tick-ms は 1 以上");
    }

    // 代 = 起動した時刻。保存なしで、立ち上がり直すたびに必ず大きくなる
    let life = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("時計が UNIX epoch より前")?
        .as_millis() as u64;

    let host = Arc::new(Host::new(FieldConfig {
        tick_interval: Duration::from_millis(tick_ms),
    }));
    let field = host.create(key.clone(), place, life)?;
    let server = FieldServer::start(host, &listen.to_string())
        .await
        .with_context(|| format!("{listen} で待ち受けられない"))?;

    println!(
        "field {key} を動かしている（場所 {place:?}、代 {life}、tick {tick_ms}ms、待ち受け {}）",
        server.local_addr()
    );
    println!("止めるには Ctrl-C");

    tokio::signal::ctrl_c().await?;
    println!();
    println!("止める");
    // 先に field を止めて、入っていた人に「止まった」を伝えてから、待ち受けをやめる
    field.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    server.shutdown().await;
    Ok(())
}

async fn watch(key: FieldKey, connect: SocketAddr) -> anyhow::Result<()> {
    let mut visit = FieldClient::join(connect, &key).await?;
    let snapshot = visit.snapshot();
    println!(
        "入った: {}/{}（場所 {:?}、代 {}、tick {}）",
        snapshot.owner, snapshot.name, snapshot.place, snapshot.life, snapshot.tick
    );
    loop {
        tokio::select! {
            note = visit.next_tick() => match note {
                Some(note) => println!("tick {}（代 {}）", note.tick, note.life),
                None => {
                    println!("field が止まった");
                    return Ok(());
                }
            },
            _ = tokio::signal::ctrl_c() => {
                visit.leave().await;
                println!();
                println!("出ていった");
                return Ok(());
            }
        }
    }
}
