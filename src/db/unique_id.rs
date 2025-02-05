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
