//! field を動かす入れ物。
//!
//! 手元では field を 1 つ、クラウドでは複数のユーザの field を、同じ入れ物で動かす。
//! モデルの `registry` に当たる。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::core::{Field, FieldKey, Place};
use crate::runtime::{FieldHandle, spawn_field};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum HostError {
    #[error("同じ持ち主の、同じ名前の field がすでにある: {0}")]
    AlreadyExists(FieldKey),
}

#[derive(Default)]
pub struct Host {
    fields: Mutex<HashMap<FieldKey, FieldHandle>>,
}

impl Host {
    pub fn new() -> Self {
        Self::default()
    }

    /// 持つ: field を作って動かし始める
    pub fn create(&self, key: FieldKey, place: Place, life: u64) -> Result<FieldHandle, HostError> {
        let mut fields = self.fields.lock().expect("fields の lock が壊れている");
        if fields.contains_key(&key) {
            return Err(HostError::AlreadyExists(key));
        }
        let handle = spawn_field(Field::new(key.clone(), place, life));
        fields.insert(key, handle.clone());
        Ok(handle)
    }

    /// 鍵から field を引く
    pub fn get(&self, key: &FieldKey) -> Option<FieldHandle> {
        self.fields
            .lock()
            .expect("fields の lock が壊れている")
            .get(key)
            .cloned()
    }

    /// ある持ち主の field の鍵（名前順）
    pub fn fields_of(&self, owner: &str) -> Vec<FieldKey> {
        let mut keys: Vec<FieldKey> = self
            .fields
            .lock()
            .expect("fields の lock が壊れている")
            .keys()
            .filter(|key| key.owner == owner)
            .cloned()
            .collect();
        keys.sort();
        keys
    }
}

#[cfg(test)]
mod tests {
    //! 名前は `docs/model/field.qnt` の場面の確かめと同じ。

    use serde_json::json;

    use super::*;

    fn mako_studio() -> FieldKey {
        FieldKey::new("mako", "studio")
    }

    fn mako_live() -> FieldKey {
        FieldKey::new("mako", "live")
    }

    fn waki_studio() -> FieldKey {
        FieldKey::new("waki", "studio")
    }

    #[tokio::test]
    async fn one_user_local_and_cloud() {
        let host = Host::new();
        host.create(mako_studio(), Place::Local, 1).unwrap();
        host.create(mako_live(), Place::Cloud, 1).unwrap();
        assert_eq!(host.fields_of("mako"), [mako_live(), mako_studio()]);
        let studio = host.get(&mako_studio()).unwrap().status().await.unwrap();
        let live = host.get(&mako_live()).unwrap().status().await.unwrap();
        assert_eq!(studio.snapshot.place, Place::Local);
        assert_eq!(live.snapshot.place, Place::Cloud);
    }

    #[tokio::test]
    async fn fields_are_independent() {
        let host = Host::new();
        let studio = host.create(mako_studio(), Place::Local, 1).unwrap();
        let live = host.create(mako_live(), Place::Cloud, 1).unwrap();
        let _presence = live.join().await.unwrap();
        studio.set("light", json!(1)).await.unwrap();
        // studio だけ止める
        studio.stop().await;
        assert!(studio.status().await.is_none());
        live.set("light", json!(2)).await.unwrap();
        let live = live.status().await.unwrap();
        assert_eq!(live.snapshot.slots["light"].rev, 1);
        assert_eq!(live.visitors, 1);
    }

    #[tokio::test]
    async fn same_name_different_owner() {
        let host = Host::new();
        let mako = host.create(mako_studio(), Place::Local, 1).unwrap();
        let waki = host.create(waki_studio(), Place::Cloud, 1).unwrap();
        let _presence = mako.join().await.unwrap();
        mako.set("light", json!(1)).await.unwrap();
        assert_eq!(mako.status().await.unwrap().visitors, 1);
        let waki = waki.status().await.unwrap();
        assert_eq!(waki.visitors, 0);
        assert!(waki.snapshot.slots.is_empty());
        assert_eq!(host.fields_of("waki"), [waki_studio()]);
    }

    #[tokio::test]
    async fn duplicate_name_fails() {
        let host = Host::new();
        host.create(mako_studio(), Place::Local, 1).unwrap();
        assert_eq!(
            host.create(mako_studio(), Place::Cloud, 1).err(),
            Some(HostError::AlreadyExists(mako_studio()))
        );
    }

    #[tokio::test]
    async fn join_missing_field_fails() {
        let host = Host::new();
        assert!(host.get(&mako_studio()).is_none());
    }
}
