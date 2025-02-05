#![allow(dead_code)]
use super::{ID_BITS, ID_MASK};
use std::sync::{Arc, RwLock};
pub(crate) fn level_id(id: u64, level: usize) -> u64 {
    (level as u64) << ID_BITS | (id & ID_MASK)
}

#[derive(Clone, Debug)]
pub struct Point<T: Clone> {
    pub(crate) id_level: u64,
    pub(crate) arrow: Option<Arc<Vec<T>>>,                          //可能处于 已经加载 或者 未加载状态
    pub(crate) neighbor: Option<Arc<RwLock<LevelVec<T>>>>,          //放在 arc rwlock 内部 已实现 内部可变性
}

impl<T: Clone> PartialEq for Point<T> {
    fn eq(&self, other: &Point<T>) -> bool {
        self.id_level == other.id_level
    }
}

impl<T: Clone> Eq for Point<T> {}

impl<T: Clone> PartialOrd for Point<T> {
    fn partial_cmp(&self, other: &Point<T>) -> Option<std::cmp::Ordering> {
        self.id_level.partial_cmp(&other.id_level)
    }
}

impl<T: Clone> Ord for Point<T> {
    fn cmp(&self, other: &Point<T>) -> std::cmp::Ordering {
        self.id_level.cmp(&other.id_level)
    }
}

impl<T: Clone> Point<T> {
    pub fn new(id: u64, level: usize) -> Self {
        Self { id_level: level_id(id, level), arrow: None, neighbor: None }
    }
    pub fn id(&self) -> u64 {
        self.id_level & ID_MASK
    }
    pub fn level(&self) -> usize {
        (self.id_level >> ID_BITS) as usize
    }
    pub fn to_order_id(&self, dist: f32) -> OrderId<T> {
        OrderId { point: self.clone(), dist }
    }
}

#[derive(Clone)]
pub struct OrderId<T: Clone> {
    pub(crate) point: Point<T>,
    pub dist: f32,
}

impl<T: Clone> std::fmt::Debug for OrderId<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.point.id(), self.dist)
    }
}

impl<T: Clone> OrderId<T> {
    pub fn new(id_level: u64, dist: f32) -> Self {
        Self { point: Point{id_level, arrow: None, neighbor: None}, dist }
    }
}

impl<T: Clone> PartialEq for OrderId<T> {
    fn eq(&self, other: &OrderId<T>) -> bool {
        self.dist == other.dist
    }
}

impl<T: Clone> Eq for OrderId<T> {}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl<T: Clone> PartialOrd for OrderId<T> {
    fn partial_cmp(&self, other: &OrderId<T>) -> Option<std::cmp::Ordering> {
        self.dist.partial_cmp(&other.dist)
    }
}

impl<T: Clone> Ord for OrderId<T> {
    fn cmp(&self, other: &OrderId<T>) -> std::cmp::Ordering {
        if !self.dist.is_nan() && !other.dist.is_nan() {
            self.dist.partial_cmp(&other.dist).unwrap()
        } else {
            panic!("got a NaN in a distance");
        }
    }
}

#[derive(Clone)]
pub struct LevelVec<T: Clone> {
    pub(crate) value: Vec<OrderId<T>>,
}

impl<T: Clone> LevelVec<T> {
    pub fn to_vec(&self)-> Vec<u8> {
        let ids: Vec<(u64, f32)> = self.value.iter().map(|v| (v.point.id_level, v.dist) ).collect();
        super::vec_to_u8(ids)
    }
}

impl<T: Clone> std::fmt::Debug for LevelVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for v in &self.value { let _ = write!(f, "{:?},", v); }
        Ok(())
    }
}

impl<T: Clone> Default for LevelVec<T> {
    fn default() -> Self {
        Self { value: Vec::with_capacity(64) }
    }
}

impl<T: Clone> LevelVec<T> {
    pub(crate) fn push(&mut self, oid: OrderId<T>, threshold: Option<usize>) -> bool {
        if self.value.iter().position(|v| v.point.id_level == oid.point.id_level).is_none() {
            let level = oid.point.level();
            self.value.push(oid);
            if let Some(threshold) = threshold {
                self.shrink(level, threshold);
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn remove_id(&mut self, id: u64)-> bool {
        if let Some(pos) = self.value.iter().position(|v| v.point.id() == id) {
            self.value.swap_remove(pos);        //应该不需要保持顺序
            true
        } else { false }
    }

    pub(crate) fn append(&mut self, other: &mut Vec<OrderId<T>>) {
        self.value.append(other);
    }

    pub(crate) fn get(&self, level: usize) -> Vec<OrderId<T>> {
        self.value.iter().filter_map(|v| if v.point.level() == level { Some(v.clone()) } else { None }).collect()
    }

    fn first(&self, level: usize) -> Option<OrderId<T>> {
        self.value.iter().find(|v| v.point.level() == level).map(|v| v.clone())
    }

    fn sort(&mut self) {
        self.value.sort_unstable();
    }
    fn len(&self, level: usize) -> usize {
        self.value.iter().fold(0, |count, v| if v.point.level() == level { count + 1 } else { count })
    }

    fn find<F: FnMut(&mut OrderId<T>) -> bool>(&mut self, level: usize, mut f: F) -> bool {
        for v in &mut self.value {
            if v.point.level() == level && f(v) {
                return true;
            }
        }
        false
    }

    fn shrink(&mut self, level: usize, threshold: usize) -> bool {
        let mut pos_value = None;
        self.value.iter().enumerate().for_each(|(pos, v)| {
            if v.point.level() == level {
                if pos_value.is_none() {
                    pos_value = Some((1, pos, v.dist));
                } else {
                    pos_value.as_mut().map(|p| {
                        if p.2 < v.dist {
                            p.2 = v.dist;
                            p.1 = pos;
                        }
                        p.0 += 1;
                    });
                }
            }
        });
        pos_value
            .map(|pos_v| {
                if pos_v.0 > threshold {
                    self.value.remove(pos_v.1);
                    true
                } else {
                    false
                }
            })
            .unwrap_or(false)
    }
}
