//! club-field — Chronista Club の field を動かすプログラム。
//!
//! 仕様: `docs/spec/01-field.md`
//! モデル: `docs/model/field.qnt`
//! 設計: `docs/design/01-field-program.md`
//!
//! 層は 2 つ:
//! - [`sync`]: Origin ↔ Slave の同期。field を知らない汎用の部品（将来 club-unison の側へ移す）
//! - field: [`core`] / [`runtime`] / [`host`] / [`protocol`] / [`client`]。`sync` を使う側

pub mod client;
pub mod core;
pub mod host;
pub mod protocol;
pub mod runtime;
pub mod sync;
