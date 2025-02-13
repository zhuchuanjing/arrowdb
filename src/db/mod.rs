//自己实现的基于 HNSW 的 向量数据库
//为什么要自己实现 因为没有一个较好的 内存和 持续化设施 结合的 HNSW 实现, usearch 是 C++的 hnsw_lib 没有考虑持续化
//基本原则 加入数据会获得一个唯一的 ID u64 应该暂时足够了
//注意 不能随便删除一个 point 因为涉及到整个导航网络 少量的数据更改 改变向量可以直接进行 导航网格可以自己调整过来

//就是一个单纯的 向量数据库 支持多个向量集合
//collection -> 创建 集合 删除 集合 获取集合列表
//向集合增加一个向量 返回向量的 id（u64)
//批量增加向量
//更改 指定 id 的向量
//删除指定 id
//获取 集合向量的数量(包括已经删除的 向量)

use anndists::dist::*;
use hnsw::HNSW;
use fjall::{Config, TxKeyspace};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use anyhow::{anyhow, Result};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Dist {
    L1,
    L2,
    Cosine,
}

impl Dist {
    pub fn eval(&self, va: &[f32], vb: &[f32]) -> f32 {
        match self {
            Self::L1=> DistL1{}.eval(va, vb),
            Self::L2=> DistL2{}.eval(va, vb),
            Self::Cosine=> DistCosine{}.eval(va, vb)
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Collection {
    dimension: usize,                   //维度
    max_layer: usize,                   //最大层数 16
    nb_conn: usize,                     //邻居
    ef: usize,                          //构建邻居
    dist: Dist,                         //距离类型
}

impl Collection {
    fn dim(dimension: usize) -> Self {
        Self{dimension, max_layer: 16, nb_conn: 20, ef: 200, dist: Dist::L2}
    }
}

use crate::store::{KVStore, fjall::FjallStore};
#[derive(Clone)]
pub struct ArrowDB  {
    space: TxKeyspace,
    //collect_partion: TxPartition,
    store: FjallStore,
    collections: Arc<RwLock<HashMap<String, Collection>>>,
    hnsws: Arc<RwLock<HashMap<String, hnsw::HNSW<FjallStore>>>>,
}

//需要有一些参数的定义 每一个集合 比如说 max 层数 维度 距离函数 临近数量等
impl ArrowDB {
    fn add_hnsw(&self, name: &str, collection: &Collection) {
        let store = FjallStore::open(&self.space, name);
        self.hnsws.write().unwrap().insert(name.into(),
            hnsw::HNSW::new(store, collection.nb_conn, collection.ef, collection.max_layer, Dist::L2)
        );
    }
    pub fn new(path: &str)-> Self{
        let space = Config::new(path).open_transactional().unwrap();
        let mut collections = HashMap::new();
        let store = FjallStore::open(&space, "#collections");
        let read_tx = space.read_tx();
        for key in read_tx.keys(&store.tx) {
            let key = key.unwrap();
            let slice = read_tx.get(&store.tx, &key).unwrap().unwrap();
            let c: Collection = rmp_serde::from_slice(&slice).unwrap();
            collections.insert(String::from_utf8(key.to_vec()).unwrap(), c);
        }
        Self{space, store, collections: Arc::new(RwLock::new(collections)), hnsws: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn get_collections(&self)-> Vec<String> {
        self.collections.read().unwrap().keys().map(|k| k.clone() ).collect()
    }

    pub fn create_collection(&self, name: &str, dimension: usize)-> Result<()> {
        let c = Collection::dim(dimension);
        self.store.set(Bytes::copy_from_slice(name.as_bytes()), Bytes::from_owner(rmp_serde::to_vec(&c).unwrap()))?;
        self.add_hnsw(name, &c);
        self.collections.write().unwrap().insert(name.into(), c);
        Ok(())
    }

    pub fn get_hnsw(&self, name: &str, dim: usize)-> Result<HNSW<FjallStore>> {
        if let Some(info) = self.collections.read().unwrap().get(name) {
            if info.dimension != dim {
                return Err(anyhow!("collection dimension {} is not equal {}", info.dimension, dim));
            }
            if !self.hnsws.read().unwrap().contains_key(name) {
                self.add_hnsw(name, info);
            }
            Ok(self.hnsws.read().unwrap().get(name).unwrap().clone())
        } else {
            Err(anyhow!("collection {} do not existed", name))
        }
    }
}

use std::mem;
pub(crate) fn u8_to_vec<T: Clone>(u8_vec: Vec<u8>) -> Vec<T> {
    let len = u8_vec.len() / mem::size_of::<T>();
    let ptr = u8_vec.as_ptr() as *const T;
    mem::forget(u8_vec);
    unsafe { Vec::from_raw_parts(ptr as *mut T, len, len) }//.clone()
}


pub(crate) fn vec_to_u8<T>(vec: Vec<T>) -> Vec<u8> {
    let len = vec.len() * mem::size_of::<T>();
    let ptr = vec.as_ptr() as *const u8;
    mem::forget(vec);
    unsafe { Vec::from_raw_parts(ptr as *mut u8, len, len) }
}

pub(crate) trait PersistID {
    fn size(&self)-> u64;
    fn get_id(&self) -> u64;
    fn entry(&self)-> (usize, u64);
    fn set_entry(&self, level: usize, id: u64);
}

pub(crate) const ID_BITS: usize = 64 - 4;              //2 的 4 次方层 最大 0-15 已经足够了
pub(crate) const ID_MASK: u64 = 0xfffffffffffffffu64;

pub mod hnsw;
mod layer;
pub mod order_id;
mod unique_id;
