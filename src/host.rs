//! field を動かす入れ物。
//!
//! 手元では field を 1 つ、クラウドでは複数のユーザの field を、同じ入れ物で動かす。
//! モデルの `registry` に当たる。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::core::{Field, FieldKey, Place};
use crate::runtime::{FieldConfig, FieldHandle, spawn_field};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum HostError {
    #[error("同じ持ち主の、同じ名前の field がすでにある: {0}")]
    AlreadyExists(FieldKey),
}

pub struct Host {
    config: FieldConfig,
    fields: Mutex<HashMap<FieldKey, FieldHandle>>,
}

impl Host {
    pub fn new(config: FieldConfig) -> Self {
        Self {
            config,
            fields: Mutex::new(HashMap::new()),
        }
    }

    /// 持つ: field を作って動かし始める
    pub fn create(&self, key: FieldKey, place: Place, life: u64) -> Result<FieldHandle, HostError> {
        let mut fields = self.fields.lock().expect("fields の lock が壊れている");
        if fields.contains_key(&key) {
            return Err(HostError::AlreadyExists(key));
        }
        let handle = spawn_field(Field::new(key.clone(), place, life), self.config);
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

    use std::time::Duration;

    use super::*;

    const INTERVAL: Duration = Duration::from_millis(100);

    fn host() -> Host {
        Host::new(FieldConfig {
            tick_interval: INTERVAL,
        })
    }

    fn mako_studio() -> FieldKey {
        FieldKey::new("mako", "studio")
    }

    fn mako_live() -> FieldKey {
        FieldKey::new("mako", "live")
    }

    fn waki_studio() -> FieldKey {
        FieldKey::new("waki", "studio")
    }

    #[tokio::test(start_paused = true)]
    async fn one_user_local_and_cloud() {
        let host = host();
        host.create(mako_studio(), Place::Local, 1).unwrap();
        host.create(mako_live(), Place::Cloud, 1).unwrap();
        assert_eq!(host.fields_of("mako"), [mako_live(), mako_studio()]);
        let studio = host.get(&mako_studio()).unwrap().status().await.unwrap();
        let live = host.get(&mako_live()).unwrap().status().await.unwrap();
        assert_eq!(studio.snapshot.place, Place::Local);
        assert_eq!(live.snapshot.place, Place::Cloud);
    }

    #[tokio::test(start_paused = true)]
    async fn fields_are_independent() {
        let host = host();
        let studio = host.create(mako_studio(), Place::Local, 1).unwrap();
        let live = host.create(mako_live(), Place::Cloud, 1).unwrap();
        let _presence = live.join().await.unwrap();
        // studio だけ止める
        studio.stop().await;
        tokio::time::sleep(INTERVAL * 2 + INTERVAL / 2).await;
        assert!(studio.status().await.is_none());
        let live = live.status().await.unwrap();
        assert_eq!(live.snapshot.tick, 2);
        assert_eq!(live.visitors, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn same_name_different_owner() {
        let host = host();
        let mako = host.create(mako_studio(), Place::Local, 1).unwrap();
        let waki = host.create(waki_studio(), Place::Cloud, 1).unwrap();
        let _presence = mako.join().await.unwrap();
        assert_eq!(mako.status().await.unwrap().visitors, 1);
        assert_eq!(waki.status().await.unwrap().visitors, 0);
        assert_eq!(host.fields_of("waki"), [waki_studio()]);
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_name_fails() {
        let host = host();
        host.create(mako_studio(), Place::Local, 1).unwrap();
        assert_eq!(
            host.create(mako_studio(), Place::Cloud, 1).err(),
            Some(HostError::AlreadyExists(mako_studio()))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn join_missing_field_fails() {
        let host = host();
        assert!(host.get(&mako_studio()).is_none());
    }
}
