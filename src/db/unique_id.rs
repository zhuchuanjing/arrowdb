//两种不同的 唯一 ID 生成器

//生成用于查询的 唯一ID 运行中使用 不需要持续化
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
pub(crate) struct QueryID {
    id: Arc<AtomicU64>,
}

const QUERY_START:  u64 = 0x0000ffffffffffffu64;
const QUERY_STOP:   u64 = 0x0fffffffffffffffu64;            //最大 4096个并发搜索
use super::order_id::{ID_BITS, ID_MASK};

impl Default for QueryID {
    fn default() -> Self {
        Self{id: Arc::new(AtomicU64::new(QUERY_START)) }
    }
}

impl QueryID {
    pub(crate) fn get(&self) -> u64 {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _ = self.id.compare_exchange(QUERY_STOP, QUERY_START, Ordering::Acquire, Ordering::Relaxed);
        id
    }
}

struct ID64<'a>(Option<&'a fjall::Slice>);
impl<'a> Into<u64> for ID64<'a> {
    fn into(self) -> u64 {
        self.0.map(|buf| u64::from_le_bytes(buf.as_ref().try_into().unwrap())).unwrap_or(0)
    }
}

struct IDSlice(fjall::Slice);
impl Into<IDSlice> for u64 {
    fn into(self) -> IDSlice {
        IDSlice(self.to_le_bytes().into())
    }
}

//持续化的 ID 多线程并发的情况下 使用 fjall 事务实现 唯一性
use fjall::TxPartition;
#[derive(Clone)]
pub(crate) struct PersistID {
    idx: TxPartition,
}

impl PersistID {
    pub fn new(idx: TxPartition)-> Self {
        Self{idx}
    }

    pub fn size(&self)-> u64 {
        ID64(self.idx.get("__id__").unwrap().as_ref()).into()
    }

    pub fn get(&self) -> u64 {
        ID64(self.idx.fetch_update("__id__", |old| {
            let old: u64 = ID64(old).into();
            let slice: IDSlice = (old + 1).into();
            Some(slice.0)
        }).unwrap().as_ref()).into()
    }

    pub fn entry(&self)-> (usize, u64) {
        let id_level: u64 = ID64(self.idx.get("__entry__").unwrap().as_ref()).into();
        ((id_level >> ID_BITS) as usize, id_level & ID_MASK)
    }

    pub fn set_entry(&self, level: usize, id: u64) {
        let _ = self.idx.fetch_update("__entry__", |old| {
            let old_val: u64 = ID64(old).into();
            if ((old_val >> ID_BITS) as usize) < level {
                let slice: IDSlice = super::order_id::level_id(id, level).into();
                Some(slice.0)
            } else {
                let slice: IDSlice = old_val.into();
                Some(slice.0)
            }
        });
    }
}
