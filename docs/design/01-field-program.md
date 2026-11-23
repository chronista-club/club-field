# CC-DESIGN-001 — field プログラム

- 状態: 実装済み（2026-11-22、U1 の範囲）。正本と写し（Origin と Slave）の自動同期に作り替えた（2026-11-23）。
  QUIC で下流を待ち受ける中継（`field relay`）を足した（2026-11-23）
- 仕様: [`docs/spec/01-field.md`](../spec/01-field.md)
- モデル: [`docs/model/field.qnt`](../model/field.qnt)
- コード: このレポ（`src/`、`schemas/`、`tests/`）

## 何を作ったか

ユーザが、コマンド 1 つで手元に自分の field を持ち、入った人の手元に field の写し（Slave）ができて、
field が自動で同期するプログラム。

```bash
cargo run -- up    --owner mako --name studio --set light=0.5   # 自分の field を持つ
cargo run -- watch --owner mako --name studio                   # 入って、手元の Slave の変動を見る
cargo run -- set   --owner mako --name studio light 0.8         # 入って、置き換えを頼む
```

`field up` の標準入力に `<要素> <値>`（例: `camera wide`）を打つと、持ち主として直接置き換えられる。
値は JSON として読み、読めなければ文字列として扱う。

中継（`Origin ──QUIC──▶ Relay ──QUIC──▶ Slave`）:

```bash
cargo run -- relay --owner mako --name studio --listen '[::1]:7881'                            # field に入り、下流を待ち受ける
cargo run -- relay --owner mako --name studio --connect '[::1]:7881' --listen '[::1]:7882'     # 中継の先にさらに中継（何段でも）
cargo run -- watch --owner mako --name studio --connect '[::1]:7882'                           # 中継を通して入る
cargo run -- set   --owner mako --name studio --connect '[::1]:7882' light 0.8                 # 中継を経て Origin で確定する
```

`watch` / `set` / `relay` のつなぐ先は、field でも中継でもよい（同じ受け答えをする）。
中継は、上流が止まると「上流が止まった」と出して下流を外し、終わる。Ctrl-C なら上流から出ていき、下流を外して終わる。

| 入っている | 入っていない |
|---|---|
| 持つ（起動する） | 何を置くか（要素の種類と中身の形） |
| 入る / 出ていく | 描画するクライアント |
| 要素を置き換える（持ち主が直接 / 入った人が頼む） | 認可（誰が頼めるか）、住所 |
| 手元の Slave が自動で同期する | クラウドの置き場 |
| 中継（Slave を下流の Slave の配り手にする。同じプロセスの中） | どの中継に入るかの案内、つなぎ替え |
| QUIC で下流を待ち受ける中継のプロセス（`field relay`。何段でも） | |
| 止まる / 立ち上がり直す | |

## 構造

```
┌─ field プログラム（1 プロセス）───────────────────────────────────┐
│  main      引数を読む。up / relay / watch / set                   │
│  host      field と中継の入れ物。鍵（持ち主 + 名前）で引く            │
│  runtime   1 field = 1 タスク。入る・出ていく・置き換える・止まる       │
│  relay     中継。上流に入っている写しを預かり、下流への配り手にする     │
│  core      field そのもの。名札と、入っている人。IO なし               │
│  protocol  入口。KDL のスキーマ + unison の channel（presence / feed） │
│  client    入って、手元に Slave を持つ。feed を裏で吸い上げる           │
├───────────────────────────────────────────────────────────────┤
│  sync      Origin ↔ Slave の同期。field を知らない汎用の部品           │
│            （将来 club-unison の側へそのまま移せる境界）               │
└───────────────────────────────────────────────────────────────┘
         ▲ QUIC（club-unison。presence は入る人が、feed は field が開く stream）
```

| 部品 | ファイル | 役割 | IO |
|---|---|---|---|
| sync | `src/sync.rs` | 要素ごとの `(rev, value)`。Origin・Slave・購読・中継 | なし（tokio の同期の道具だけ） |
| core | `src/core.rs` | field の名札・入っている人・動いているか。要素は中の Origin | なし |
| runtime | `src/runtime.rs` | field を 1 タスクで動かす | なし |
| relay | `src/relay.rs` | 中継。上流に入っている写し（`FieldSlave`）を預かり、下流が入る・頼むに応える | なし（預かった写しの上流への口が、ネットワークを使う） |
| host | `src/host.rs` | field と中継の入れ物 | なし |
| protocol | `src/protocol.rs`、`schemas/field.kdl` | `Join` / `Set` と `Update` の受け答え。field にも中継にも同じ道で入れる | ネットワーク |
| client | `src/client.rs` | 入って Slave を持つクライアント | ネットワーク |
| main | `src/main.rs` | `field up` / `field relay` / `field watch` / `field set` | 引数、端末 |

`sync` は field の語彙（持ち主・名前・場所・`FieldKey`）を一切使わない。field の側から `sync` を使い、
`sync` から field の側は使わない。

## 決めたこと

| # | 決めたこと | 内容 | 理由 |
|---|---|---|---|
| D1 | 言語と通信 | Rust + tokio + club-unison | Field サーバは Rust（2026-08-14 の mako の指定）。unison はこのために作った道具 |
| D2 | コードの置き場 | `chronista-club/club-field`（このレポ） | ポータル（Web）とは道具も出し方も別なので、レポを分けた |
| D3 | 名前 | crate `club-field`、コマンド `field` | `club-unison` / `club-kdl` と揃える。Ladyland の `fieldd` とぶつからない |
| D4 | プロセスと field | 1 プロセスが複数の field を持てる（1 field = 1 タスク）。`field up` は 1 つだけ動かす | 手元とクラウドで同じプログラムにする |
| D5 | 変動の道のり | 順番どおり・新しいものだけ・応答の道あり・受け取る側で捨てる（モデルの `streamLatestMerge`） | モデルで、常に今の状態（どの道でも破れる）以外が全部成り立つ。応答の道を足しても合流する |
| D6 | 「新しいものだけ」の作り | 購読（`sync::Subscription`）が、変わった要素の索引と、`tokio::sync::watch` の知らせ（通し番号だけ）から、前回より後に変わった要素の今の値を引く | 購読者ごとの待ち行列が無い。溜まるのは要素の数まで。古いものは stream に書き込む前に畳まれる |
| D7 | 知らせに載せるもの | 要素 1 つぶんの `(life, slot, rev, value)`。値は丸ごと | 間引いても追いつける。前の代を捨てられる |
| D8 | 代の決め方 | 起動した時刻（ミリ秒） | 保存なしで、立ち上がり直すたびに必ず大きくなる |
| D9 | 同期の契機 | 要素が置き換えられたときだけ（変動駆動）。時計（tick）はやめた | 裁定 13。変わっていないものを送り続けない |
| D10 | 持ち主 | `--owner` で名乗る。確かめない | 手元専用の段階 |
| D11 | 待ち受け先 | loopback だけ。それ以外を指定すると起動しない | 認可なしで外に開かない |
| D12 | 保存 | しない | spec のとおり |
| D13 | ポート番号 | 既定は `[::1]:7880`。**仮** | 台帳（creo-port-ssot）で正式に取るまでの仮置き |
| D14 | 道を 2 本に分ける | `presence`（入る人が開く。`Join` / `Set`）と `feed`（field が開く。`Update`） | 入る人が開く channel の event は、受け取る側の待ち行列（256 件）が満ちると黙って捨てられる（club-unison の `UnisonChannel`）。field が開く stream は受け取る側が直接読むので、取りこぼさず、遅ければ QUIC の流量制御が送る側を待たせる |
| D15 | Slave の合流の決まり | 同じ代で、手元より新しい rev だけを受け入れる（`Slots::merge`） | 道がいくつあっても（応答、要素ごとの stream、中継）合流する。モデルの反例 2・4・5 を防ぐ |
| D16 | 頼んだ人は応答でも合流させる | `Slave::set` は、確定した変動を応答で受け取り、手元にも合流させてから返る | 頼んだ側は、`set` が返った時点で確定した値を持つ（知らせを待たない）。知らせと順番が入れ替わっても D15 で戻らない |
| D17 | 同期を汎用の部品に分ける | `src/sync.rs`。field の語彙を使わない | 裁定 13。将来 club-unison の側（`club-unison-remote` など）へそのまま移す |
| D18 | 値の形 | JSON（`serde_json::Value`） | 中身の形（U2）がまだ決まっていない。unison の既定の codec も JSON |
| D19 | 要素の名前 | 空でなければ自由。空は断る（`bad_request`） | 中身の形と一緒に決める |
| D20 | 中継の形 | `Origin` も `Slave` も `Source`（購読と、頼みの行き先）になる。`Slave::follow(&source)` で下流の Slave を作る | Slave も Origin と同じ形の状態を持つので、配り方も同じでよい。rev は Origin が振ったまま運ぶ |
| D21 | 同時に出す頼みの数 | 1 つの Slave（presence 1 本）から同時に出す `Set` は 128 まで。超えたぶんは手元で待つ | field の側は presence の request を待ち行列（256 件）から 1 つずつ処理し、満ちると黙って捨てる（下の「実装して分かったこと」）。中継が下流の頼みをまとめて運ぶと、すぐ満ちる |
| D22 | QUIC の中継の置き方 | 上流に入った `FieldSlave` を `Host::relay` で預け、field と並べて置く（`Hosted::Field` / `Hosted::Relay`）。鍵は上流の名札（持ち主 + 名前）。protocol は、どちらにも同じ道（presence / feed）・同じ受け答えで入れる | 下流から見て field と区別が要らない（`watch` / `set` / 次の中継がそのまま使える）。`host.create` と同じ手触り。sync を field の語彙で汚さない（中継が使うのは `Source::subscribe` / `Source::upstream` だけ） |
| D23 | 中継が下流の頼みを運ぶ順番 | presence 1 本ごとに別のタスクで応対し、1 本の中では届いた順に 1 つずつ上流へ運ぶ。運ぶ間、lock は握らない | ある下流の頼みが上流の返事を待っていても、ほかの下流の `Join` / `Set` は待たされない。1 人の頼みの順番は保たれる（モデルの「1 人の頼みは、その人の道で順に届く」） |
| D24 | 中継が断る頼み | 空の要素名は、上流へ運ぶ前に断る（`bad_request`）。上流が断った理由はそのまま返す。上流に届かなかった頼みは `unreachable` | 無駄に上流まで往復しない。何段重ねても、断る理由は Origin のものが届く。`unreachable` は確定したかどうか分からない（確定していれば feed で届く）ので、`stopped` と分けた |
| D25 | 外れたら、返事を待っている頼みも終わる | `Slave::set`（`sync::Link`）は、上流の返事を待つ間に Slave が上流から外れたら、待つのをやめて `Closed` を返す | unison の `UnisonChannel::close` は返事待ちの request を解かず、request の時間切れ（30 秒）まで待たせる（下の「実装して分かったこと」）。中継が止まったとき、下流の頼みを抱えたタスクを残さない |
| D26 | `field relay` の待ち受け先 | `--listen` は必須（既定なし）。ポート 0 なら空いているポート。loopback だけ（D11） | 中継は上流と同じマシンで動かすことが多く、`field up` の既定（`[::1]:7880`）とはぶつかる。既定のポート自体が仮（D13）なので、仮のポートをもう 1 つ増やさない |

D1〜D5 は mako の承認（2026-11-22「ok」）。D6〜D13 は claude の案で、同じ承認のもとで進めた。
D5〜D7 の見直し、D9、D14〜D21 は、2026-11-23 の mako の裁定（GO、「あとはrecommendでOK」）のもとで claude が決めた。
D22〜D26 は、2026-11-23 の mako の裁定（「`field relay` を先に小さく通してから `club-unison` へ輸出」、
`mem_1ChKHwzGspREaztdkR7CDP`）を受けた lead の段 B の task のもとで claude が決めた。

D2 と D3 は、当初 `chronista.club` レポの `field/` に置く案で承認され、そのとおり実装した。
その後「ポータルとサーバが同居している」ことから、マージ前に別レポへ移すと mako が決め、
名前は `club-field` になった（2026-11-22）。外に公開する名前は `club-` で揃える、という
chronista-club の決まり（2026-05-15）に合わせている。

### 前の版（tick）から変えた点

| 点 | 前 | いま | 理由 |
|---|---|---|---|
| field の中身 | 時計（tick）だけ | 名前の付いた要素の集まり。要素ごとに `(rev, value)` | 裁定 13 |
| 送る契機 | 一定間隔（`--tick-ms`） | 要素が置き換えられたとき | 裁定 13 |
| 入った人の手元 | 届いた `Tick` を 1 件ずつ読む（`FieldClient::next_tick`） | 手元の Slave を読む。Slave は裏で自動同期する | 裁定 9 |
| 入った人からの更新 | 無い | `Set` で頼み、Origin が確定する | 裁定 11 |
| 道 | `presence` 1 本（`Tick` は presence の event） | `presence` と `feed` の 2 本 | D14 |

## 流れるデータ

スキーマの正本は [`schemas/field.kdl`](../../schemas/field.kdl)。

| 操作 | 道 | 向き | 中身 |
|---|---|---|---|
| `Join`（request） | presence | 入る人 → field | `owner`、`name` |
| `Join` の応答 | presence | field → 入る人 | その時点の状態（名札と要素）。入れなかったら `error` |
| `Set`（request） | presence | 入った人 → field | `slot`、`value` |
| `Set` の応答 | presence | field → 頼んだ人 | 確定した変動 `(life, slot, rev, value)`。確定しなかったら `error` |
| `Update`（event） | feed | field → 入っている人 | 要素 1 つぶんの変動 `(life, slot, rev, value)` |
| channel を閉じる | presence | どちらからでも | 入る人が閉じる = 出ていく。field が閉じる = 止まった（feed も閉じる） |

`Join` の応答（状態 1 つ）の形:

```json
{
  "owner": "mako", "name": "studio", "place": "local", "life": 1795407248953,
  "slots": { "light": { "rev": 2, "value": 0.8 }, "camera": { "rev": 1, "value": "wide" } }
}
```

`Update` と `Set` の応答の形:

```json
{ "life": 1795407248953, "slot": "light", "rev": 3, "value": 0.5 }
```

入るときの順番:

1. 入る人は、`feed` の受け口を置いてから（`register_server_channel`）つなぎ、`presence` を開いて `Join` を送る
2. field は入る人を受け付け（その時点の状態と、以後の変動の購読を作る）、`feed` を開き
   （`open_server_stream`）、購読から `feed` へ書く係を立ててから、`Join` に応答する。
   つまり `Update` が `Join` の応答より先に届くことがある
3. 入る人は、応答の状態から Slave を作り、`feed` を吸い上げる係を立てる。
   応答より先に届いていた `Update` は、`feed` の中で待っていて、このとき合流する

`Join` の応答と `feed`、`Set` の応答と `feed` は、別の stream なので届く順番は決まらない。
受け取る側（Slave）が D15 の決まりで合流させる。

### 中継を通すとき

```
Origin ──QUIC──▶ Relay ──QUIC──▶ Leaf
 field            field relay      field watch / field set / 次の field relay
```

| 操作 | 中継で起きること |
|---|---|
| `Join` | 中継の手元の状態（名札と代は field のもの）を返し、中継の Slave の購読から `feed` へ書く係を立てる。中継の手元は、上流より遅れていることがある |
| `Set` | 中継の Slave から上流へ `Set` を運ぶ（D23）。上流で確定した変動は、応答の道で中継の Slave に合流し、下流へは応答と `feed` の両方で届く |
| 上流が止まる・切れる | 中継の Slave が閉じ、下流の `feed` と presence を中継の側から閉じる。以後は入れない（`stopped`） |
| 中継が止まる | 中継の Slave を手放して上流から出ていき、下流の `feed` と presence を閉じる。Origin と、ほかの中継・直結の Slave は動き続ける |

Origin から見ると、中継は入っている人 1 人。下流が何人いても、Origin が書く `feed` は中継 1 本ぶん。

`feed` の受け口を置いていないクライアント（たとえば unison の MCP から `Join` だけ呼ぶ）も入れるが、
`feed` は受け取られずに捨てられる。field の側は、最初の変動を `feed` に書けなかった時点で
presence も閉じる（出ていったことになる）。`feed` を失ったまま入っている人は残らない。

## モデルとの対応

| Quint のモデル | 実装 |
|---|---|
| `registry` | `Host` |
| `Field { place, running, life, slots }` | `core::Field`（`slots` と `life` は中の `sync::Origin`） |
| `SlotState { rev, value }` | `sync::SlotState` |
| `isJoined` / `joined` | `core::Field::visitors` |
| `seen`（手元の Slave） | `sync::Slave`（client では `client::FieldSlave`） |
| `via`（中継） | `Slave::follow(&slave)` で作った下流の Slave。QUIC では、`Host::relay` に預けた `FieldSlave`（`relay::RelayHandle`）に入った下流の Slave |
| `create` | `Host::create`（`field up`） |
| `join` | `Join` → `FieldHandle::join` → `core::Field::join` |
| `joinVia` | `Slave::follow(&relay)`。QUIC では、`field relay` の待ち受けに `Join` → `RelayHandle::join` |
| `leave` | presence が閉じる → `Presence` を手放す → `core::Field::leave`。Slave を手放す → 下流の購読が終わる。中継が止まる（`RelayHandle::stop`）→ 中継の Slave を手放す |
| `setSlot` | `FieldHandle::set`（`field up` の標準入力） |
| `requestSet` / `commit` | `Slave::set` → `Set` → `FieldHandle::set` → `sync::Origin::set`。中継を通すときは `Set` → `RelayHandle::set` → 中継の Slave の上流への口 → `Set` → … |
| `receive`（Feed） | client の吸い上げる係 → `Inlet::apply` → `Slots::merge` |
| `receive`（Reply） | `Slave::set` が応答を `Slots::merge` |
| `stop` | `FieldHandle::stop`（`field up` の Ctrl-C）→ `Origin::close` |
| `restart` | もう一度 `field up`。代は起動時刻なので前より大きい |
| `streamLatest` の間引き | 購読が、変わった要素の今の値だけを返す |
| `DISCARD_OLDER` / `CHECK_LIFE` | `Slots::merge` |

テストの名前は、できるだけモデルの場面の確かめに揃えてある（`slotsMergeInAnyOrderTest` → `slots_merge_in_any_order`）。

## sync の部品

| 型 | 役割 |
|---|---|
| `Slots` | 要素の集まり（IO なし）。`set` で確定（Origin）、`merge` で合流（Slave）、`changes_since` で「ある時点より後に変わった要素の今の値」 |
| `Origin` | 正本。`set` / `get` / `snapshot` / `subscribe` / `close`。`subscribe` は、購読した時点の状態と購読を別々に返す |
| `Subscription` | 購読。`next()` で、購読した時点より後に変わった要素の今の値（途中の rev は畳まれる）。状態の写しは持たない（入った人の数だけ写しを抱えないため） |
| `Slave` | 手元の写し。`get` / `snapshot` / `changed` / `set`。手放すと上流から外れる。外れると、上流の返事を待っている `set` も `Closed` で終わる（D25） |
| `Inlet` | 運び方の受け取り係が、届いた変動を Slave に流し込む口（`apply` / `close` / `slave_gone`） |
| `Upstream` | 置き換えの頼みの行き先。運び方ごとに実装する（client の `QuicUpstream`、`Origin` 自身） |
| `Source` | 配り手（`subscribe` と `upstream`）。`Origin` と `Slave` が実装する |

club-unison の側へ移すときは、`src/sync.rs` をそのまま移し、`protocol.rs` / `client.rs` の運び方
（`Join` / `Set` / `Update` と feed の開き方）を、field の名札を外した汎用の channel にする。
unison の側の構想（`mem_1CgkvMWbDW7Tne5DwRxhjD`）の「control は client 起点、feed は server 起点」と同じ形にしてある。

## テスト

```bash
cargo test
```

| 段 | 場所 | 件数 | 内容 |
|---|---|---|---|
| sync | `src/sync.rs` | 26 | 合流の決まり（古い rev・同じ rev・前の代を捨てる、要素の順不同）、畳み込み、応答と知らせの入れ替わり、遅い利用者、中継（多段・下流からの頼み・中継が外れる）、外れたら返事待ちの頼みも終わる |
| core | `src/core.rs` | 17 | 操作ごとの振る舞い。IO なし |
| runtime | `src/runtime.rs` | 11 | 受け取りが遅い人には最新だけが残る、戻らない、先に進まない、止まる |
| relay | `src/relay.rs` | 7 | 中継に入る → 中継の状態と以後の変動、頼みが上流で確定して中継にも合流する、上流の返事を待つ頼みがあっても入れて頼める、出ていった人は数えない、上流が止まる・中継が止まると下流が外れる、止めると返事待ちの頼みも終わる。IO なし（同じプロセスの Origin を上流にする） |
| host | `src/host.rs` | 7 | 複数の field、同名・別の持ち主、重複、中継を field と並べて置く、中継と field は同じ鍵に置けない |
| 結合 | `tests/presence.rs` | 23 | 本物の QUIC。入る → 状態、Origin の変動が Slave に届く、Slave からの頼みが確定して他の Slave にも届く、何人もが入り混じって置き換えても全員が揃う、遅い利用者でも Slave は最新、QUIC の Slave を中継にする、出ていく、止まる、別々の field、断られる頼み、1 つの Slave から 3000 の頼みを一度に出しても全部確定する、feed を受け取らないクライアントは外される、入るのに失敗してもつながりが残らない。**QUIC の中継**（`Origin ──QUIC──▶ Relay ──QUIC──▶ Leaf`）: Origin の変動が Leaf に届く、Leaf の頼みが Origin で確定して中継・別の Leaf・直結の Slave に揃う、何人もが入り混じって置き換えても揃う、Leaf が出ていっても残りは動く、中継が止まるとその下流だけ外れる、Origin が止まると中継にも下流にも伝わり入れなくなる、3 段（`Origin → Relay 1 → Relay 2 → Leaf`）で上り下りが通る、上流の返事を待つ頼みがあってもほかの Leaf は入れて頼める（`slow` にだけ返事をしない偽の上流で） |

結合テストは、続けて 15 回、6 本並べて 5 回（計 30 回）回して、落ちなかった（2026-11-23、macOS）。

## 実装して分かったこと

### 入る人が開く channel の event は、黙って捨てられることがある

club-unison 2.3 の `UnisonChannel` は、届いた event を受け取る側の待ち行列（256 件）に入れ、
満ちていると捨てる（`channel.rs` の `try_deliver`）。request の応答を止めないための作りで、
event は「取りこぼしうる」扱いになっている。

前の版の `Tick` はこの道を通っていた。`Tick` は状態を丸ごと運ぶので次の `Tick` で追いつけたが、
要素ごとの変動は、ある要素の最後の変動が捨てられると、その要素が次に変わるまで古いまま
（モデルの反例 3 と同じ形）。そこで `feed` を field の側から開く stream に移した（D14）。
こちらは受け取る側が直接読むので、取りこぼさない。

### 入る人が開く channel の request も、黙って捨てられることがある

field の側（`UnisonChannel`）は、presence に届いた request も event と同じ待ち行列（256 件）に入れ、
満ちていると捨てる。field は request を 1 つずつ処理するので、1 つの Slave から一度に大量の `Set` を出すと、
あふれたぶんは確定されず、頼んだ側は応答を待ったまま 30 秒で時間切れになる
（moody-blues のレビューで測った値: 一度に 3000 で 474、6000 で 1008 が捨てられた）。
捨てられた頼みは確定していないので、状態は食い違わない。困るのは、捨てられることと 30 秒待つこと。

クライアントの側で、同時に出す `Set` を 128 までに抑えた（D21）。結合テストで、一度に 3000 出して全部確定する。
抑えているのは 1 つの Slave の中だけで、presence を何本も開く相手には効かない（field の側で request を
並行に処理する、待ち行列を大きくする、などは別の話）。

### channel を閉じても、返事待ちの request は解かれない

club-unison 2.3 の `UnisonChannel::close` は、受け取り係を止めて stream を閉じるが、返事を待っている request
（`pending`）は解かない。request は時間切れ（30 秒）まで待ち続ける（つながりが切れたときは、受け取り係が
全部を error で解く）。

中継を止めると、中継の Slave の吸い上げる係が上流への presence を閉じる。そのとき、下流の頼みを上流へ運んで
返事を待っていた中継の側のタスクは、30 秒まで残りうる（club-unison のコードを読んでの見立て。時間は測っていない）。
そこで `Slave::set` が、Slave が上流から外れた時点で待つのをやめるようにした（D25）。
確かめているのは sync の `pending_set_ends_when_slave_is_closed` / `pending_relayed_set_ends_when_relay_leaves` と
`relay::tests::stop_ends_pending_set`（どれも、直す前は時間切れで落ちた）。結合テストの
`relay_waiting_upstream_holds_up_no_other_leaf` は、下流の側の頼みが終わることだけを見ている
（下流の側は、中継が presence を閉じた時点で、つながりの終わりとして解かれる）。

### 下流からは、中継が止まったのか field が止まったのか区別できない

中継が止まっても、その上流が止まっても、下流には feed と presence が閉じられることしか届かない。
`field watch` はどちらも「field が止まった」と表示する。理由を運ぶ道は、いまは無い。

### 遅い利用者がいても、Slave は最新

手元の利用者が `changed()` を呼ばなくても、Slave は裏で `feed` を吸い上げ続ける。
結合テストで、利用者が見ないまま 200 回置き換えると、Slave は rev 200 になっており、
利用者が見に来たときに受け取るのは、畳まれた最新の 1 つだけだった（`slow_consumer_slave_is_always_current`）。

送る側（field）で畳まれるのは、`feed` への書き込みが詰まったときに限られる。
いまの大きさ（loopback、値が小さい）では詰まらず、送る側で畳まれる様子は測っていない。

### 確かめていないこと

- 自分のマシンの外からの接続（loopback 以外では起動しない）
- 長時間の動作、多数の接続（1000 人など）、多数の中継・深い段数（確かめたのは 3 段まで）
- 中継が上流に届かない（`unreachable`）場面。結合テストでは起こしていない（request の時間切れ 30 秒を待つため）
- 中継を通した下流の頼みの速さ。1 本の presence の頼みは 1 つずつ上流へ運ぶので、1 人あたり 1 往復ずつかかる（D23）。測っていない
- クラウドに置いたときの動き

## この先

| 項目 | 内容 |
|---|---|
| unison へ移す | `src/sync.rs` と運び方を club-unison の側（`club-unison-remote` など）へ。名前と channel 名は unison の側の構想と合わせて決める。中継（`relay.rs` と、protocol が field と中継を同じ道で配る形）も一緒に |
| 中継の案内 | どの中継に入るかを案内する・中継が止まったら別の中継や Origin へつなぎ替える。住所・認可と一緒に |
| 要素ごとの道 | 要素ごとに `feed` を分ける・優先度をつける。合流の決まり（D15）はそのまま使える |
| 書き手と読み手 | 頼める人を絞る（演者だけが書く）。認可と一緒に |
| 認可 | club-unison の接続時の認証（`design/connection-auth.md`）に、持ち主を確かめる処理をつなぐ |
| クラウドに置く | 同じプログラムを置く。認可と証明書が先に要る |
| ポート番号 | 台帳で正式に取る |
| 何を置くか | U2。要素の種類と中身の形 |
| ブラウザから入る | 同じサーバに WebTransport の入口を足す。2026-11-21 の unison の調べ（`mem_1CgkvMWbDW7Tne5DwRxhjD`）では、TS / Swift のクライアントは相手から開かれる stream を identity の 1 本しか受けない。`feed` を受けられるようにするのが先 |
