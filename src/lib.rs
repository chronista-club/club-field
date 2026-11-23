//! club-field — Chronista Club の field を動かすプログラム。
//!
//! 仕様: `docs/spec/01-field.md`
//! モデル: `docs/model/field.qnt`
//! 設計: `docs/design/01-field-program.md`
//!
//! 層は 2 つ:
//! - [`sync`]: Origin ↔ Slave の同期と、その QUIC の運び方・中継。field を知らない汎用の部品で、
//!   club-unison の `unison::sync` をそのまま使う（`field::sync` はその別名）
//! - field: [`core`] / [`runtime`] / [`relay`] / [`host`] / [`protocol`] / [`client`]。
//!   名札（持ち主・名前・場所）と入れ物、field の入口（presence / feed）だけを持ち、`sync` を使う

pub mod client;
pub mod core;
pub mod host;
pub mod protocol;
pub mod relay;
pub mod runtime;

pub use unison::sync;
