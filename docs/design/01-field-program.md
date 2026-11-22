# CC-DESIGN-001 — field プログラム

- 状態: 実装済み（2026-11-22、U1 の範囲）
- 仕様: [`docs/spec/01-field.md`](../spec/01-field.md)
- モデル: [`docs/model/field.qnt`](../model/field.qnt)
- コード: このレポ（`src/`、`schemas/`、`tests/`）

## 何を作ったか

ユーザが、コマンド 1 つで、手元に自分の field を持てるプログラム。

```bash
cargo run -- up --owner mako --name studio        # 自分の field を持つ
cargo run -- watch --owner mako --name studio     # 入って tick を見る（確かめ用）
```

| 入っている | 入っていない |
|---|---|
| 持つ（起動する） | field の中身と「置く」 |
| 入る / 出ていく | 描画するクライアント |
| tick が届く | 認可、住所 |
| 止まる / 立ち上がり直す | クラウドの置き場 |

## 構造

```
┌─ field プログラム（1 プロセス）──────────────────────────────┐
│  main      引数を読む。持ち主・名前・場所・待ち受け先           │
│  host      field を動かす入れ物。鍵（持ち主 + 名前）で引く      │
│  runtime   1 field = 1 タスク。時計を刻み、最新の状態を送り出す  │
│  core      field そのもの。IO なし。モデルを写したもの          │
│  protocol  入口。KDL のスキーマ + unison の channel           │
│  client    入って tick を受け取る、確かめ用の小さなクライアント   │
└────────────────────────────────────────────────────────────┘
         ▲ QUIC（club-unison の stream channel）
```

| 部品 | ファイル | 役割 | IO |
|---|---|---|---|
| core | `src/core.rs` | field の状態と操作 | なし |
| runtime | `src/runtime.rs` | field を 1 タスクで動かす | 時計 |
| host | `src/host.rs` | field の入れ物 | なし |
| protocol | `src/protocol.rs`、`schemas/field.kdl` | `Join` と `Tick` の受け答え | ネットワーク |
| client | `src/client.rs` | 確かめ用のクライアント | ネットワーク |
| main | `src/main.rs` | `field up` / `field watch` | 引数、端末 |

## 決めたこと

| # | 決めたこと | 内容 | 理由 |
|---|---|---|---|
| D1 | 言語と通信 | Rust + tokio + club-unison | Field サーバは Rust（2026-08-14 の mako の指定）。unison はこのために作った道具 |
| D2 | コードの置き場 | `chronista-club/club-field`（このレポ） | ポータル（Web）とは道具も出し方も別なので、レポを分けた |
| D3 | 名前 | crate `club-field`、コマンド `field` | `club-unison` / `club-kdl` と揃える。Ladyland の `fieldd` とぶつからない |
| D4 | プロセスと field | 1 プロセスが複数の field を持てる（1 field = 1 タスク）。`field up` は 1 つだけ動かす | 手元とクラウドで同じプログラムにする |
| D5 | tick の道のり | 順番どおり・新しいものだけ（stream channel） | モデルで破れる性質が一番少なく、受け取る側の決まりが要らない |
| D6 | 「新しいものだけ」の作り | `tokio::sync::watch`（最新の 1 枚だけを持つ）から、送る係が stream へ書く | 古いものは stream に書き込む前に捨てられる |
| D7 | 知らせに載せるもの | その時点の状態を丸ごと（持ち主・名前・場所・代・tick） | 間引いても追いつける。立ち上がり直しに気づける |
| D8 | 代の決め方 | 起動した時刻（ミリ秒） | 保存なしで、立ち上がり直すたびに必ず大きくなる |
| D9 | tick の間隔 | `--tick-ms`、既定 1000 | 中身が無い間は速くする意味が無い |
| D10 | 持ち主 | `--owner` で名乗る。確かめない | 手元専用の段階 |
| D11 | 待ち受け先 | loopback だけ。それ以外を指定すると起動しない | 認可なしで外に開かない |
| D12 | 保存 | しない | spec のとおり |
| D13 | ポート番号 | 既定は `[::1]:7880`。**仮** | 台帳（creo-port-ssot）で正式に取るまでの仮置き |

D1〜D5 は mako の承認（2026-11-22「ok」）。D6〜D13 は claude の案で、同じ承認のもとで進めた。

D2 と D3 は、当初 `chronista.club` レポの `field/` に置く案で承認され、そのとおり実装した。
その後「ポータルとサーバが同居している」ことから、マージ前に別レポへ移すと mako が決め、
名前は `club-field` になった（2026-11-22）。外に公開する名前は `club-` で揃える、という
chronista-club の決まり（2026-05-15）に合わせている。

### Conception のときの案から変えた点

| 点 | 案 | 実装 | 理由 |
|---|---|---|---|
| `Join` の中身 | 空 | `owner` と `name` を渡す | 1 プロセスが複数の field を持てるので、どの field かを言う必要がある |
| 入れなかったとき | 未定 | 応答の `error` に理由を入れる（`no_such_field` / `stopped` / `already_joined` / `bad_request`） | — |
| `field watch` | 無かった | 足した | 「外から入って確かめる」ための道具。描画はしない |
| club-unison の版 | 2.1 | `"2.1"` の指定で 2.3.0 が入った | crates.io の最新。手元のレポ（2.1.0）より新しい |

## 流れるデータ

スキーマの正本は [`schemas/field.kdl`](../../schemas/field.kdl)。

| 操作 | 向き | 中身 |
|---|---|---|
| `Join`（request） | 入る人 → field | `owner`、`name` |
| `Join` の応答 | field → 入る人 | その時点の状態。入れなかったら `error` |
| `Tick`（event） | field → 入っている人 | その時点の状態を丸ごと |
| channel を閉じる | どちらからでも | 入る人が閉じる = 出ていく。field が閉じる = 止まった |

状態 1 つの形:

```json
{ "owner": "mako", "name": "studio", "place": "local", "life": 1795294287963, "tick": 12 }
```

届く順番の保証: 応答を先に stream へ書いてから tick を流し始める。同じ stream なので、
入った人には必ず「状態 → それより後の tick」の順で届く。

## モデルとの対応

| Quint のモデル | 実装 |
|---|---|
| `registry` | `Host` |
| `Field { place, tick, running, life }` | `core::Field` |
| `isJoined` / `joined` | `core::Field::visitors` |
| `create` | `Host::create`（`field up`） |
| `join` | `Join` → `FieldHandle::join` → `core::Field::join` |
| `leave` | channel が閉じる → `Presence` を手放す → `core::Field::leave` |
| `tick` | runtime の時計 → `core::Field::tick` |
| `receive` | `FieldClient::next_tick`（入る側） |
| `stop` | `FieldHandle::stop`（`field up` の Ctrl-C） |
| `restart` | もう一度 `field up`。代は起動時刻なので前より大きい |
| `streamLatest` の間引き | `watch` が最新の 1 枚だけを持つ |

テストの名前は、モデルの場面の確かめに揃えてある（`tickAdvancesWithNobodyTest` → `tick_advances_with_nobody`）。

## テスト

```bash
cargo test
```

| 段 | 場所 | 件数 | 内容 |
|---|---|---|---|
| core | `src/core.rs` | 15 | 操作ごとの振る舞い。IO なし |
| runtime | `src/runtime.rs` | 9 | 時間を止めて進める。受け取りが遅い人には最新だけが残る、戻らない、先に進まない |
| host | `src/host.rs` | 5 | 複数の field、同名・別の持ち主、重複 |
| 結合 | `tests/presence.rs` | 6 | 本物の QUIC。入る → 状態 → tick、出ていく、止まる、別々の field |

## 実装して分かったこと

### 「新しいものだけ」は、いまの大きさでは働かない

結合テストで、受け取らないまま 25 回ぶん待ってから読むと、**25 個すべてが届いた**（飛ばされたのは 0）。

- 知らせが数十バイトと小さいので、stream への書き込みが詰まらない。送る係は毎回すぐ書けてしまう
- さらに、unison のクライアントは届いた event を自分の待ち行列に溜める。読む側が遅くても、
  その遅れは送る側まで伝わりにくい
- つまり、いまの実際の振る舞いは、モデルでいう `streamAll`（全部届ける）に近い

仕組み（`watch`）は入っており、runtime のテストでは最新だけが残ることを確かめている。
実際に間引きが働くのは、書き込みが詰まるほど知らせが大きい・速いときに限られる。

この先に効いてくること:

- 描画するクライアントを作るときは、**受け取る側でも「最新だけを使う」**作りが要る
  （溜まった event を順に全部処理すると、遅れが広がる）
- 知らせに中身が入って大きくなったら、もう一度測る

### 確かめていないこと

- 自分のマシンの外からの接続（loopback 以外では起動しない）
- 長時間の動作、多数の接続
- クラウドに置いたときの動き

## この先

| 項目 | 内容 |
|---|---|
| クラウドに置く | 同じプログラムを置く。認可と証明書が先に要る |
| 認可 | club-unison の接続時の認証（`design/connection-auth.md`）に、持ち主を確かめる処理をつなぐ |
| ポート番号 | 台帳で正式に取る |
| field の中身 | U2。「置く」操作と、知らせの中身 |
| ブラウザから入る | 同じサーバに WebTransport の入口を足す |
