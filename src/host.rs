//! field を動かす入れ物。
//!
//! 手元では field を 1 つ、クラウドでは複数のユーザの field を、同じ入れ物で動かす。
//! モデルの `registry` に当たる。
//!
//! field（正本）と並べて、中継（上流の field に入っている写し）も置ける。
//! protocol は、どちらにも同じ道で下流を入れる。

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use crate::client::FieldSlave;
use crate::core::{Field, FieldKey, Place};
use crate::relay::RelayHandle;
use crate::runtime::{FieldHandle, spawn_field};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum HostError {
    #[error("同じ持ち主の、同じ名前の field がすでにある: {0}")]
    AlreadyExists(FieldKey),
}

/// 入れ物に置いてあるもの 1 つ。field そのもの（正本）か、中継（上流の field の写し）か
#[derive(Clone)]
pub enum Hosted {
    Field(FieldHandle),
    Relay(RelayHandle),
}

#[derive(Default)]
pub struct Host {
    entries: Mutex<HashMap<FieldKey, Hosted>>,
}

impl Host {
    pub fn new() -> Self {
        Self::default()
    }

    /// 持つ: field を作って動かし始める
    pub fn create(&self, key: FieldKey, place: Place, life: u64) -> Result<FieldHandle, HostError> {
        let mut entries = self.lock();
        if entries.contains_key(&key) {
            return Err(HostError::AlreadyExists(key));
        }
        let handle = spawn_field(Field::new(key.clone(), place, life));
        entries.insert(key, Hosted::Field(handle.clone()));
        Ok(handle)
    }

    /// 中継する: 上流の field に入っている写しを預かり、同じ鍵（上流の持ち主 + 名前）で下流を入れる
    pub fn relay(&self, slave: FieldSlave) -> Result<RelayHandle, HostError> {
        self.add_relay(RelayHandle::new(slave))
    }

    pub(crate) fn add_relay(&self, relay: RelayHandle) -> Result<RelayHandle, HostError> {
        let mut entries = self.lock();
        if entries.contains_key(relay.key()) {
            return Err(HostError::AlreadyExists(relay.key().clone()));
        }
        entries.insert(relay.key().clone(), Hosted::Relay(relay.clone()));
        Ok(relay)
    }

    /// 鍵から field（正本）を引く。中継は引かない
    pub fn get(&self, key: &FieldKey) -> Option<FieldHandle> {
        match self.find(key)? {
            Hosted::Field(field) => Some(field),
            Hosted::Relay(_) => None,
        }
    }

    /// 鍵から、置いてあるもの（field か中継）を引く
    pub fn find(&self, key: &FieldKey) -> Option<Hosted> {
        self.lock().get(key).cloned()
    }

    /// ある持ち主の、置いてあるもの（field と中継）の鍵（名前順）
    pub fn fields_of(&self, owner: &str) -> Vec<FieldKey> {
        let mut keys: Vec<FieldKey> = self
            .lock()
            .keys()
            .filter(|key| key.owner == owner)
            .cloned()
            .collect();
        keys.sort();
        keys
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<FieldKey, Hosted>> {
        self.entries.lock().expect("入れ物の lock が壊れている")
    }
}

#[cfg(test)]
mod tests {
    //! 名前は `docs/model/field.qnt` の場面の確かめと同じ。

    use serde_json::json;

    use super::*;
    use crate::sync::{Origin, Relay, Slave};

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
        assert!(host.find(&mako_studio()).is_none());
    }

    /// 同じプロセスの Origin に入っている Slave を、中継として預かる
    fn relay_of(origin: &Origin, key: &FieldKey) -> RelayHandle {
        let info = crate::core::FieldInfo {
            owner: key.owner.clone(),
            name: key.name.clone(),
            place: Place::Local,
            life: origin.life(),
        };
        RelayHandle::hold(info, Relay::from_slave(Slave::follow(origin)))
    }

    #[tokio::test]
    async fn relay_sits_beside_fields() {
        let host = Host::new();
        host.create(mako_live(), Place::Cloud, 1).unwrap();
        let origin = Origin::new(1);
        // 中継は、上流の field と同じ鍵で置かれる
        let relay = host.add_relay(relay_of(&origin, &mako_studio())).unwrap();
        assert_eq!(relay.key(), &mako_studio());
        assert!(matches!(host.find(&mako_studio()), Some(Hosted::Relay(_))));
        assert!(matches!(host.find(&mako_live()), Some(Hosted::Field(_))));
        // get は field（正本）だけを引く
        assert!(host.get(&mako_studio()).is_none());
        assert_eq!(host.fields_of("mako"), [mako_live(), mako_studio()]);
    }

    #[tokio::test]
    async fn relay_and_field_cannot_share_a_key() {
        let host = Host::new();
        let origin = Origin::new(1);
        host.create(mako_studio(), Place::Local, 1).unwrap();
        assert_eq!(
            host.add_relay(relay_of(&origin, &mako_studio())).err(),
            Some(HostError::AlreadyExists(mako_studio()))
        );
        host.add_relay(relay_of(&origin, &waki_studio())).unwrap();
        assert_eq!(
            host.add_relay(relay_of(&origin, &waki_studio())).err(),
            Some(HostError::AlreadyExists(waki_studio()))
        );
        assert_eq!(
            host.create(waki_studio(), Place::Local, 1).err(),
            Some(HostError::AlreadyExists(waki_studio()))
        );
    }
}
