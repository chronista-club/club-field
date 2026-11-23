//! `field` コマンド。
//!
//! - `field up`    自分の field を持つ（動かし始める）。標準入力の `<要素> <値>` で要素を置き換える
//! - `field relay` 動いている field（または中継）に入り、中継として下流を待ち受ける
//! - `field watch` 動いている field に入り、手元の Slave の変動を表示する（確かめ用）
//! - `field set`   動いている field に入り、要素の置き換えを頼む（確かめ用）
//!
//! `watch` / `set` / `relay` のつなぐ先は、field でも中継でもよい（同じ受け答えをする）。
//!
//! 値は JSON として読む。JSON として読めなければ文字列として扱う
//! （`0.8` は数、`true` は真偽値、`calm` は文字列 `"calm"`）。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use field::client::FieldSlave;
use field::core::{FieldKey, Place};
use field::host::Host;
use field::protocol::FieldServer;
use field::runtime::FieldHandle;
use field::sync::{SlotUpdate, Subscription};

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
    /// 自分の field を持つ（動かし始める）。標準入力の `<要素> <値>` で要素を置き換える。Ctrl-C で止まる
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
        /// 初めに置く要素（`<要素>=<値>`、何度でも）
        #[arg(long = "set", value_name = "SLOT=VALUE")]
        initial: Vec<String>,
    },
    /// 動いている field（または中継）に入り、中継として下流を待ち受ける。Ctrl-C で止まる
    Relay {
        /// 持ち主
        #[arg(long)]
        owner: String,
        /// field の名前
        #[arg(long)]
        name: String,
        /// 上流（field、または別の中継）
        #[arg(long, default_value = DEFAULT_ADDR)]
        connect: SocketAddr,
        /// 待ち受け先（例: `[::1]:7881`）。上流と同じマシンで動かすことが多いので、既定は置かない。
        /// ポート 0 なら空いているポートを使う。認可がまだ無いので、loopback だけ
        #[arg(long)]
        listen: SocketAddr,
    },
    /// 動いている field に入り、手元の Slave の変動を表示する（確かめ用）
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
    /// 動いている field に入り、要素の置き換えを頼む（確かめ用）
    Set {
        /// 持ち主
        #[arg(long)]
        owner: String,
        /// field の名前
        #[arg(long)]
        name: String,
        /// つなぐ先
        #[arg(long, default_value = DEFAULT_ADDR)]
        connect: SocketAddr,
        /// 要素の名前
        slot: String,
        /// 値（JSON。読めなければ文字列）
        value: String,
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
            initial,
        } => up(FieldKey::new(owner, name), place.into(), listen, initial).await,
        Command::Relay {
            owner,
            name,
            connect,
            listen,
        } => relay(FieldKey::new(owner, name), connect, listen).await,
        Command::Watch {
            owner,
            name,
            connect,
        } => watch(FieldKey::new(owner, name), connect).await,
        Command::Set {
            owner,
            name,
            connect,
            slot,
            value,
        } => {
            set(
                FieldKey::new(owner, name),
                connect,
                slot,
                parse_value(&value),
            )
            .await
        }
    }
}

/// 値は JSON として読む。読めなければ文字列
fn parse_value(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
}

fn show(update: &SlotUpdate) -> String {
    format!("{} = {}（rev {}）", update.slot, update.value(), update.rev)
}

/// 待ち受け先は loopback だけ（認可がまだ無いので）
fn ensure_loopback(listen: SocketAddr) -> anyhow::Result<()> {
    if !listen.ip().is_loopback() {
        bail!(
            "{listen} は自分のマシンの外から届く。認可がまだ無いので、loopback（例: {DEFAULT_ADDR}）だけにしてある"
        );
    }
    Ok(())
}

/// 変動を表示し続ける。配り手が止まったら終わる
async fn print_changes(mut changes: Subscription) {
    while let Some(updates) = changes.next().await {
        for update in &updates {
            println!("{}", show(update));
        }
    }
}

async fn up(
    key: FieldKey,
    place: Place,
    listen: SocketAddr,
    initial: Vec<String>,
) -> anyhow::Result<()> {
    ensure_loopback(listen)?;
    let initial = initial
        .iter()
        .map(|pair| {
            pair.split_once('=')
                .filter(|(slot, _)| !slot.is_empty())
                .map(|(slot, value)| (slot.to_string(), parse_value(value)))
                .with_context(|| format!("--set は <要素>=<値> の形で: {pair}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // 代 = 起動した時刻。保存なしで、立ち上がり直すたびに必ず大きくなる
    let life = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("時計が UNIX epoch より前")?
        .as_millis() as u64;

    let host = Arc::new(Host::new());
    let field = host.create(key.clone(), place, life)?;
    for (slot, value) in initial {
        field.set(slot, value).await?;
    }
    let server = FieldServer::start(host, &listen.to_string())
        .await
        .with_context(|| format!("{listen} で待ち受けられない"))?;

    println!(
        "field {key} を動かしている（場所 {place:?}、代 {life}、待ち受け {}）",
        server.local_addr()
    );
    for (slot, state) in field
        .status()
        .await
        .context("field が止まった")?
        .snapshot
        .slots
    {
        println!("  {slot} = {}（rev {}）", state.value(), state.rev);
    }
    println!("要素を置き換えるには `<要素> <値>` を入力。止めるには Ctrl-C");

    // 誰が置き換えても（手元でも、入っている人からの頼みでも）、変動をここに表示する
    let changes = field.observe().await.context("field が止まった")?;
    tokio::spawn(print_changes(changes));

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut reading = true;
    loop {
        tokio::select! {
            line = lines.next_line(), if reading => match line {
                Ok(Some(line)) => put_line(&field, &line).await,
                // 標準入力が閉じても、field は Ctrl-C まで動き続ける
                _ => reading = false,
            },
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    println!();
    println!("止める");
    // 先に field を止めて、入っていた人に「止まった」を伝えてから、待ち受けをやめる
    field.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    server.shutdown().await;
    Ok(())
}

/// 標準入力の 1 行（`<要素> <値>`）で、要素を置き換える
async fn put_line(field: &FieldHandle, line: &str) {
    let line = line.trim();
    if line.is_empty() {
        return;
    }
    let Some((slot, value)) = line.split_once(char::is_whitespace) else {
        eprintln!("`<要素> <値>` の形で入力する（例: light 0.8）");
        return;
    };
    if let Err(error) = field.set(slot, parse_value(value.trim())).await {
        eprintln!("置き換えられない: {error}");
    }
}

async fn relay(key: FieldKey, connect: SocketAddr, listen: SocketAddr) -> anyhow::Result<()> {
    ensure_loopback(listen)?;
    let slave = FieldSlave::join(connect, &key)
        .await
        .with_context(|| format!("{connect} の field {key} に入れない"))?;
    let host = Arc::new(Host::new());
    let relay = host.relay(slave)?;
    let server = match FieldServer::start(host, &listen.to_string()).await {
        Ok(server) => server,
        Err(error) => {
            relay.stop().await;
            return Err(error.context(format!("{listen} で待ち受けられない")));
        }
    };

    let info = relay.info().clone();
    println!(
        "field {key} を中継している（場所 {:?}、代 {}、上流 {connect}、待ち受け {}）",
        info.place,
        info.life,
        server.local_addr()
    );
    // 中継の手元の状態と、以後の変動（上流から届いたものも、下流の頼みで確定したものも）を表示する
    let changes = relay.observe();
    for (slot, state) in relay.snapshot().map(|s| s.slots).unwrap_or_default() {
        println!("  {slot} = {}（rev {}）", state.value(), state.rev);
    }
    println!("止めるには Ctrl-C");

    // 上流が止まる・切れると、変動の購読が終わる
    let upstream_stopped = match changes {
        Some(changes) => tokio::select! {
            _ = print_changes(changes) => true,
            _ = tokio::signal::ctrl_c() => false,
        },
        None => true,
    };
    if upstream_stopped {
        println!("上流が止まった");
    } else {
        println!();
        println!("止める");
    }
    // 上流から出ていき、下流を全員外す（上流が止まったときは、外れ終わっている）。
    // 下流に「止まった」が伝わるのを待ってから、待ち受けをやめる
    relay.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    server.shutdown().await;
    Ok(())
}

async fn watch(key: FieldKey, connect: SocketAddr) -> anyhow::Result<()> {
    let mut slave = FieldSlave::join(connect, &key).await?;
    let info = slave.info().clone();
    println!(
        "入った: {}/{}（場所 {:?}、代 {}）",
        info.owner, info.name, info.place, info.life
    );
    for (slot, state) in slave.snapshot().slots {
        println!("  {slot} = {}（rev {}）", state.value(), state.rev);
    }
    loop {
        tokio::select! {
            changed = slave.changed() => match changed {
                Some(updates) => {
                    for update in &updates {
                        println!("{}", show(update));
                    }
                }
                None => {
                    println!("field が止まった");
                    return Ok(());
                }
            },
            _ = tokio::signal::ctrl_c() => {
                slave.leave().await;
                println!();
                println!("出ていった");
                return Ok(());
            }
        }
    }
}

async fn set(key: FieldKey, connect: SocketAddr, slot: String, value: Value) -> anyhow::Result<()> {
    let slave = FieldSlave::join(connect, &key).await?;
    let confirmed = slave.set(&slot, value).await;
    slave.leave().await;
    let confirmed = confirmed.context("置き換えられなかった")?;
    println!("{}", show(&confirmed));
    Ok(())
}
