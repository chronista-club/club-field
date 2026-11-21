# club-field

Chronista Club の **field** を動かすプログラム。

field は、状態を持って動き続ける場。ユーザは自分の field を持ち、
そこに入った人は、その時点の状態と、以後の変化（tick）を受け取る。

## 状態

まだ始めたばかり。いま動くのは「自分の field を手元で持つ」まで。

| できること | まだ無いもの |
|---|---|
| field を持つ（起動する） | field の中身と「置く」操作 |
| 入る / 出ていく | 描画するクライアント |
| tick が届く | 認可、住所 |
| 止まる / 立ち上がり直す | クラウドの置き場 |

待ち受けは自分のマシンの中（loopback）だけ。認可がまだ無いため。

## 動かす

```bash
cargo run -- up --owner mako --name studio        # 自分の field を持つ
cargo run -- watch --owner mako --name studio     # 別の端末で。入って tick を見る
```

## 確かめる

```bash
cargo test              # プログラムのテスト
docs/model/check.sh     # モデルの検査（Quint。bun が要る）
```

## 文書

| 何 | どこ |
|---|---|
| 仕様（何を、なぜ） | [docs/spec/01-field.md](docs/spec/01-field.md) |
| モデル（どう振る舞うか。Quint） | [docs/model/field.qnt](docs/model/field.qnt) |
| 設計（どう作ったか） | [docs/design/01-field-program.md](docs/design/01-field-program.md) |

## ライセンス

MIT
