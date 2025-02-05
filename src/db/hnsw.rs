#![allow(dead_code)]
use bytes::{Bytes, BytesMut};
use scc::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use super::order_id::{Point, OrderId, LevelVec};
use super::layer::LayerGenerator;
use super::unique_id::QueryID;
use super::PersistID;
use crate::store::KVStore;
use anyhow::Result;
use super::Dist;

#[derive(Clone)]
pub struct HNSW<T: KVStore + Clone> {
    max_nb: usize,
    ef: usize,
    layer_g: Arc<Mutex<LayerGenerator>>,
    query_id: QueryID,
    pub(crate) dist_f: Dist,
    arrows: Arc<HashMap<u64, Arc<Vec<f32>>>>,                     //每个 id 的向量数据
    neighbors: Arc<HashMap<u64, Arc<RwLock<LevelVec<f32>>>>>,     //每个 id 的邻居数据
    store: T,
}

use rustc_hash::{FxHashSet, FxHashMap};
use std::collections::BinaryHeap;
impl<T: KVStore + Clone> HNSW<T> {
    pub fn new(store: T, max_nb: usize, ef: usize, max_level: usize, dist_f: Dist) -> Self {
        Self {
            max_nb,
            ef,
            layer_g: Arc::new(Mutex::new(LayerGenerator::new(max_nb, max_level))),
            query_id: QueryID::default(),
            dist_f,
            arrows: Arc::new(HashMap::new()),
            neighbors: Arc::new(HashMap::new()),
            store,
        }
    }

    pub fn set_arrow(&self, id: u64, arrow: Vec<f32>)-> Result<()> {
        self.arrows.update(&id, |_, v| *v = Arc::new(arrow) );
        self.save_arrow(id)
    }

    fn get_id(prefix: &[u8], id: u64)-> Bytes {
        let mut b = BytesMut::with_capacity(16);
        b.extend_from_slice(prefix);
        b.extend_from_slice(&id.to_le_bytes());
        b.freeze()
    }

    pub fn remove(&self, id: u64) {
        let (_, entry_id) = self.store.entry();
        if id != entry_id {
            let _ = self.store.remove(HNSW::<T>::get_id(b"A", id));
            self.arrows.remove(&id);
        }
    }

    fn get_arrow(&self, id: u64) -> Result<Arc<Vec<f32>>> {
        if !self.arrows.contains(&id) {
            let slice = self.store.get(HNSW::<T>::get_id(b"A", id))?;
            let arrow: Vec<f32> = super::u8_to_vec(slice.to_vec());
            let _ = self.arrows.insert(id, Arc::new(arrow));
        }
        Ok(self.arrows.get(&id).unwrap().get().clone())
    }

    fn get_neighbor(&self, point: &mut Point<f32>)-> Result<Arc<RwLock<LevelVec<f32>>>> {
        if point.neighbor.is_none() {
            let id = point.id();
            if !self.neighbors.contains(&id) {
                let slice = self.store.get(HNSW::<T>::get_id(b"N", id))?;
                let ids: Vec<(u64, f32)> = super::u8_to_vec(slice.to_vec());
                let mut neighbor = LevelVec::<f32>::default();
                ids.into_iter().for_each(|(id, dist)| { neighbor.push(OrderId::new(id, dist), None); });
                let _ = self.neighbors.insert(id, Arc::new(RwLock::new(neighbor)));
            }
            let neighbor = self.neighbors.get(&id).unwrap().get().clone();
            point.neighbor.replace(neighbor.clone());
            Ok(neighbor)
        } else {
            Ok(point.neighbor.as_ref().map(|n| n.clone() ).unwrap())
        }
    }

    fn distance(&self, point: &mut Point<f32>, other: &mut Point<f32>)-> Result<f32> {
        if point.arrow.is_none() {
            let arrow = self.get_arrow(point.id())?;
            point.arrow.replace(arrow);
        }
        if other.arrow.is_none() {
            let arrow = self.get_arrow(other.id())?;
            other.arrow.replace(arrow);
        }
        Ok(self.dist_f.eval(point.arrow.as_ref().map(|a| a.as_slice()).unwrap(), other.arrow.as_ref().map(|a| a.as_slice()).unwrap()))
    }

    fn search_layer(&self, id: &mut Point<f32>, entry: &mut Point<f32>, ef: usize, level: usize) -> Result<BinaryHeap<OrderId<f32>>> {
        let skiplist_size = ef.max(2);
        let mut return_points = BinaryHeap::<OrderId<f32>>::with_capacity(skiplist_size);
        let dist_to_entry = self.distance(id, entry)?;
        let mut visited = FxHashSet::<u64>::default(); //HashSet::<u64>::new();
        visited.insert(entry.id());
        let mut candidate = BinaryHeap::<OrderId<f32>>::with_capacity(skiplist_size);
        candidate.push(entry.to_order_id(-dist_to_entry));
        return_points.push(entry.to_order_id(dist_to_entry));
        while !candidate.is_empty() {
            let mut c = candidate.pop().unwrap();
            let f = return_points.peek().unwrap();
            if -(c.dist) > f.dist && return_points.len() >= ef {
                return Ok(return_points);
            }
            let neighbor = self.get_neighbor(&mut c.point)?.read().unwrap().get(level);
            for mut n in neighbor {
                if !visited.contains(&n.point.id()) {
                    visited.insert(n.point.id());
                    let opt = return_points.peek();
                    if opt.is_none() {
                        return Ok(return_points);
                    }
                    if let Ok(e_dist_to_p) = self.distance(id, &mut n.point){
                        let f_dist_to_p = opt.unwrap().dist;
                        if e_dist_to_p < f_dist_to_p || return_points.len() < ef {
                            let e_prime = n.point.to_order_id(e_dist_to_p);
                            candidate.push(n.point.to_order_id(-e_dist_to_p));
                            return_points.push(e_prime);
                            if return_points.len() > ef {
                                return_points.pop();
                            }
                        }
                    }
                }
            }
        }
        Ok(return_points)
    }

    fn select_neighbor(&self, id: &mut Point<f32>, candidates: &mut BinaryHeap<OrderId<f32>>, asked: usize, extend: bool) -> Result<Vec<OrderId<f32>>> {
        let mut extend_candidates = false;
        let mut neighbor = Vec::new();
        if candidates.len() <= asked {
            if !extend {
                while !candidates.is_empty() {
                    let p = candidates.pop().unwrap();
                    assert!(-p.dist >= 0.);
                    neighbor.push(p.point.to_order_id(-p.dist));
                }
                return Ok(neighbor);
            } else {
                extend_candidates = true;
            }
        }
        if extend_candidates {
            let mut candidates_set = candidates.iter().fold(FxHashMap::<u64, Point<f32>>::default(), |mut set, c| {
                set.insert(c.point.id(), c.point.clone());
                set
            });
            let keys: FxHashSet<u64> = candidates_set.keys().map(|k| k.clone() ).collect();
            let mut new_candidates_set = FxHashMap::<u64, Point<f32>>::default();
            for (_, mut cp) in &mut candidates_set {
                let cp_neighbor = self.get_neighbor(&mut cp)?.read().unwrap().get(id.level());
                for p in cp_neighbor {
                    let pid = p.point.id();
                    if !new_candidates_set.contains_key(&pid) && !keys.contains(&pid) {
                        new_candidates_set.insert(pid, p.point.clone());
                    }
                }
            }
            for (_, mut point) in new_candidates_set {
                if let Ok(dist) = self.distance(id, &mut point) {
                    candidates.push(point.to_order_id(dist));
                }
            }
        }
        while !candidates.is_empty() && neighbor.len() < asked {
            if let Some(mut e_p) = candidates.pop() {
                if neighbor.iter_mut().position(|n|
                    self.distance(&mut e_p.point, &mut n.point).map(|dist| dist <= -e_p.dist ).unwrap_or(false)
                ).is_none() {
                    neighbor.push(e_p.point.to_order_id(-e_p.dist));
                }
            }
        }
        Ok(neighbor)
    }

    fn reverse_update_neighbor(&self, point: &mut Point<f32>) -> Result<Vec<u64>> {
        let mut updated = Vec::new();
        let neighbor = self.get_neighbor(point)?.read().unwrap().clone();
        for mut n in neighbor.value {
            if n.point.level() <= point.level() && n.point.id() != point.id() {
                let threshold = if point.level() > 0 { self.max_nb } else { 2 * self.max_nb };
                if self.get_neighbor(&mut n.point)?.write().unwrap().push(point.to_order_id(n.dist), Some(threshold)) {
                    updated.push(n.point.id());
                }
            }
        }
        Ok(updated)
    }

    fn save_neighbor(&self, id: u64)-> Result<()> {
        let nid = HNSW::<T>::get_id(b"N", id);
        let buf = Bytes::from_owner(self.neighbors.get(&id).unwrap().read().unwrap().to_vec());
        self.store.set(nid, buf)
    }

    fn save_arrow(&self, id: u64)-> Result<()> {
        let aid = HNSW::<T>::get_id(b"A", id);
        let arrow = self.get_arrow(id)?.as_ref().clone();
        self.store.set(aid, Bytes::from_owner(super::vec_to_u8(arrow)))
    }

    pub fn insert(&self, arrow: Vec<f32>) -> Result<u64> {
        let id = self.store.get_id();
        let _ = self.arrows.insert(id, Arc::new(arrow));
        self.save_arrow(id)?;
        let _ = self.neighbors.insert(id, Arc::new(RwLock::new(LevelVec::default())));
        if id == 0 {
            let _ = self.save_neighbor(id);
            return Ok(id);
        }
        let level = self.layer_g.lock().unwrap().generate();
        let mut id = Point::new(id, level);
        let (max_level_observed, entry) = self.store.entry();
        let mut entry = Point::new(entry, level);
        let mut dist_to_entry = self.distance(&mut id, &mut entry)?;
        for l in ((level + 1)..(max_level_observed + 1)).rev() {
            let mut sorted_points = self.search_layer(&mut id, &mut entry, 1, l)?;
            if let Some(mut ep) = sorted_points.pop() {
                let tmp_dist = self.distance(&mut id, &mut ep.point)?;
                if tmp_dist < dist_to_entry {
                    entry = ep.point.clone();
                    dist_to_entry = tmp_dist;
                }
                self.get_neighbor(&mut id)?.write().unwrap().push(ep, None);
            }
        }
        for l in (0..level + 1).rev() {
            let ef = self.ef;
            let sorted_points = self.search_layer(&mut id, &mut entry, ef, l)?;
            let mut sorted_points: BinaryHeap<OrderId<f32>> = sorted_points.into_iter().map(|p| p.point.to_order_id(-p.dist)).collect();
            if !sorted_points.is_empty() {
                let mut nb_conn = self.max_nb;
                let mut extend_c = false;
                if l == 0 {
                    nb_conn = 2 * nb_conn;
                    extend_c = true;
                }
                let mut neighbor = self.select_neighbor(&mut id, &mut sorted_points, nb_conn, extend_c)?;
                neighbor.sort();
                if neighbor.len() > 0 {
                    entry = neighbor[0].point.clone();
                }
                self.get_neighbor(&mut id)?.write().unwrap().append(&mut neighbor);
            }
        }
        let _ = self.save_neighbor(id.id());                               //在这里写入 id 和 updated 的 id 到持续化介质
        for _id in self.reverse_update_neighbor(&mut id)? {
            let _ = self.save_neighbor(_id);
        }
        self.store.set_entry(level, id.id());
        Ok(id.id())
    }

    pub fn search(&self, data: Vec<f32>, number: usize) -> Result<Vec<(u64, f32)>> {
        if self.store.size() == 0 {
            return Ok(Vec::new());
        }
        let (level, pivot) = self.store.entry();
        let mut pivot = Point::new(pivot, level);
        let mut qid = Point::new(self.query_id.get(), level);
        qid.arrow = Some(Arc::new(data));
        let dist = self.distance(&mut qid, &mut pivot)?;
        let mut pivot_id = pivot.to_order_id(dist);
        for level in (1..=level).rev() {
            let neighbor = self.get_neighbor(&mut pivot_id.point)?.read().unwrap().get(level);
            for mut n in neighbor {
                let tmp_dist = self.distance(&mut qid, &mut n.point)?;
                if tmp_dist < pivot_id.dist {
                    pivot_id = n.point.to_order_id(tmp_dist);
                }
            }
        }
        let ef = self.ef.max(number);
        let neighbors_heap = self.search_layer(&mut qid, &mut pivot, ef, 0)?;
        let mut neighbors = neighbors_heap.into_sorted_vec();
        neighbors.truncate(number.min(ef));
        let ids: Vec<(u64, f32)> = neighbors.into_iter().map(|p| (p.point.id(), p.dist) ).collect();
        Ok(ids)
    }
}
