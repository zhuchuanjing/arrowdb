//需要提供一个底层的 KV Store
use anyhow::Result;
use bytes::Bytes;
use crate::db::{ID_BITS, ID_MASK};
//需要定义一个
pub trait KVStore {
    fn get(&self, key: Bytes)-> Result<Bytes>;
    fn set(&self, key: Bytes, value: Bytes)-> Result<()>;
    fn remove(&self, key: Bytes)-> Result<()>;
    fn update<F: Fn(Bytes)-> Bytes>(&self, key: Bytes, f: F)-> Bytes;
}

fn bytes_to_u64(buf: Bytes)-> u64 {
    buf.as_ref().try_into().map(|buf| u64::from_le_bytes(buf) ).unwrap_or(0)
}

fn u64_to_bytes(value: u64)-> Bytes {
    Bytes::copy_from_slice(value.to_le_bytes().as_ref())
}

use crate::db::PersistID;
const ID_KEY: Bytes = Bytes::from_static(b"__id__");
const ENTRY_KEY: Bytes = Bytes::from_static(b"__entry__");

impl<T: KVStore> PersistID for T {
    fn size(&self)-> u64 {                           //获取总的 ID 数目 不精确包括了已删除的
        self.get(ID_KEY).map(|buf| bytes_to_u64(buf) ).unwrap_or(0)
    }

    fn get_id(&self) -> u64 {                           //获取一个新的 ID
        bytes_to_u64(self.update(ID_KEY, |old| {
            let id = bytes_to_u64(old);
            u64_to_bytes(id + 1)
        }))
    }

    fn entry(&self)-> (usize, u64) {                 //获取入口点
        let id_level = self.get(ENTRY_KEY).map(|buf| bytes_to_u64(buf) ).unwrap_or(0);
        ((id_level >> ID_BITS) as usize, id_level & ID_MASK)
    }

    fn set_entry(&self, level: usize, id: u64) {      //设置入口点
        self.update(ENTRY_KEY, |old| {
            let old_val = bytes_to_u64(old);
            if ((old_val >> ID_BITS) as usize) < level { u64_to_bytes(super::db::order_id::level_id(id, level)) }
            else { u64_to_bytes(old_val) }
        });
    }
}

pub mod fjall;
