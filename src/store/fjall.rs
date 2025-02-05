use bytes::Bytes;
impl super::KVStore for FjallStore {
    fn get(&self, key: Bytes)-> Result<Bytes> {
        let value = Bytes::copy_from_slice(self.tx.get(&key)?.ok_or(anyhow!("no key {:?}", key))?.as_ref());
        Ok(value)
    }

    fn set(&self, key: Bytes, value: Bytes)-> Result<()> {
        Ok(self.tx.insert(key.as_ref(), value.as_ref())?)
    }

    fn remove(&self, key: Bytes)-> Result<()> {
        Ok(self.tx.remove(key.as_ref())?)
    }

    fn update<F: Fn(Bytes)-> Bytes>(&self, key: Bytes, f: F)-> Bytes {
        self.tx.fetch_update(key.as_ref(), |old| {
            let old = if let Some(old) = old { Bytes::copy_from_slice(old) } else { Bytes::default() };
            let val = f(old);
            if val.is_empty() { None }
            else { Some(val.as_ref().into()) }
        }).unwrap().map(|old| Bytes::copy_from_slice(&old)).unwrap_or(Bytes::default())
    }
}

use fjall::{PartitionCreateOptions, TxKeyspace, TxPartition};
use anyhow::{anyhow, Result};

#[derive(Clone)]
pub struct FjallStore {
    pub(crate) tx: TxPartition,
}

impl FjallStore {
    pub fn open(space: &TxKeyspace, name: &str)-> Self {
        let tx = space.open_partition(&format!("#{}", name), PartitionCreateOptions::default()).unwrap();
        Self{tx}
    }
}

#[cfg(test)]
mod tests {
    use fjall::Config;
    use crate::db::PersistID;

    use super::FjallStore;
    #[test]
    fn test_unique_id() {
        let space = Config::new("test_db").open_transactional().unwrap();
        let store = FjallStore::open(&space, "god");
        for id in 0..100 {
            println!("{} {} ", id, store.get_id());
        }
        let start = store.size();
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let s = store.clone();
            tasks.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    s.get_id();
                }
            }));
        }
        for t in tasks {
            let _ = t.join();
        }
        println!("{} {:?}", start, store.size());
    }
}
