# club-field

Chronista Club の **field** を動かすプログラム。

field は、状態を持って動き続ける場。ユーザは自分の field を持つ。
field の中身は、名前の付いた要素（slot）の集まりで、要素ごとに値と番号（rev）を持つ。

field に入った人の手元には、field の写し（Slave）ができる。Slave は field（Origin）と自動で同期し、
手元は通信を意識せず、Slave を読み、Slave の変動を待つだけでよい。手元から置き換えたいときは、
Slave から頼むと、Origin が確定して全員に届ける。

## 状態

まだ始めたばかり。いま動くのは「自分の field を手元で持ち、入った人の手元に写しができる。写しは QUIC で、さらに下流へ中継できる」まで。

| できること | まだ無いもの |
|---|---|
| field を持つ（起動する） | 何を置くか（要素の種類と中身の形） |
| 入る / 出ていく | 描画するクライアント |
| 要素を置き換える（持ち主が直接 / 入った人が頼む） | 認可（誰が頼めるか）、住所 |
| 手元の写し（Slave）が自動で同期する | クラウドの置き場 |
| 写しを、さらに下流の写しへ中継する（同じプロセスの中） | どの中継に入るかの案内、つなぎ替え |
| QUIC で下流を待ち受ける中継（`field relay`。何段でも） | |
| 止まる / 立ち上がり直す | |

待ち受けは自分のマシンの中（loopback）だけ。認可がまだ無いため。

## 動かす

```bash
cargo run -- up    --owner mako --name studio --set light=0.5   # 自分の field を持つ
cargo run -- watch --owner mako --name studio                   # 別の端末で。入って、手元の写しの変動を見る
cargo run -- set   --owner mako --name studio light 0.8         # 別の端末で。入って、置き換えを頼む
```

`field up` の端末に `<要素> <値>`（例: `camera wide`）を打つと、持ち主として直接置き換えられる。
値は JSON として読み、読めなければ文字列として扱う。

中継（`Origin ──QUIC──▶ Relay ──QUIC──▶ Slave`）:

```bash
cargo run -- relay --owner mako --name studio --listen '[::1]:7881'                   # field に入り、下流を待ち受ける
cargo run -- watch --owner mako --name studio --connect '[::1]:7881'                  # 中継を通して入る
cargo run -- set   --owner mako --name studio --connect '[::1]:7881' light 0.8        # 中継を経て field で確定する
```

中継の先に、さらに中継を重ねられる（`--connect` に中継の待ち受け先）。
中継は、上流が止まると下流を外して終わる。Ctrl-C なら上流から出ていき、下流を外して終わる。

## 確かめる

```bash
cargo test              # プログラムのテスト（結合テストは本物の QUIC を loopback で張る）
docs/model/check.sh     # モデルの検査（Quint。bun が要る。3 分ほど）
```

## 文書

| 何 | どこ |
|---|---|
| 仕様（何を、なぜ） | [docs/spec/01-field.md](docs/spec/01-field.md) |
| モデル（どう振る舞うか。Quint） | [docs/model/field.qnt](docs/model/field.qnt) |
| 設計（どう作ったか） | [docs/design/01-field-program.md](docs/design/01-field-program.md) |

## 構成

| 層 | ファイル | 役割 |
|---|---|---|
| 同期（汎用） | `src/sync.rs` | Origin ↔ Slave の同期。field を知らない。将来 club-unison の側へ移す |
| field | `src/core.rs` ほか | 名札（持ち主・名前・場所）、入る・出ていく・止まる、中継（`src/relay.rs`）、QUIC の入口 |

## ライセンス

MIT
