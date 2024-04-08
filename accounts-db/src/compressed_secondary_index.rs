use std::{hash::Hasher, mem::transmute, ops::RangeBounds};

use bv::BitsExt;
use dashmap::{lock::RwLock, DashMap, DashSet};
use solana_sdk::pubkey::{self, Pubkey};


pub (crate) fn u64_prefix(msg: &[u8]) -> u64 {
  if msg.len() >= 8 {
    u64::from_ne_bytes(*msg[0..8])
  } else {
    let mut pad = [0u8; 8];
    pad[0..msg.len()].copy_from_slice(msg);
    u64::from_ne_bytes(pad)
  }
}

pub (crate) fn prefix_to_pubkey(p: u64) -> Pubkey {
  let mut bytes = [0u8; 32];
  bytes[0..8].copy_from_slice(&p.to_ne_bytes());
  Pubkey::new_from_array(bytes)
}

pub (crate) fn prefix_to_bound(p: u64) -> RangeBounds<Pubkey> {
  let begin = Bound::Included(prefix_to_pubkey(p));
  let end = if p < u64::MAX {
    Bound::Excluded(prefix_to_pubkey(p+1))
  } else {
    Bound::Included(Pubkey::from([1u8; 32]))
  };
  return RangeBounds::new(begin, end)
}

// TODO: finish implementation
pub (crate) fn group_prefixes(prefix_set: &mut Vec<u64>, group_size_bits: u8) -> Vec<(RangeBounds<Pubkey>, Vec(u64))> {

  // pre-sort prefixes, so they are easier to group
  prefix_set.sort();

  let group_size = 1u64 << group_size_bits;
  let mask = group_size - 1;

  // TODO there's a second upper bound defined by group_size
  let mut groups = Vec::new_with_capacity(prefix_set.len());

  let mut last_group = 0;
  for prefix in prefix_set {
    if *prefix & mask != last_group {
      // start a new group
      let start = *prefix & !mask;
      let end = prefix.checked_add(group_size).map(|p| (p & !mask) - 1);
      let range = RangeBounds::new(Bound::Included(start), Bound::Included(end.unwrap_or(u64::MAX)));
      groups.push((range, vec![]));
      last_group = prefix & mask;
    }

    groups.last().unwrap().1.push(prefix);
  }
  groups
}

pub (crate) struct PrefixHasher { 
  hash: u64
}

// TODO: read hashbrown::hash_map
impl Hasher for PrefixHasher {
  #[inline]
  fn write(&mut self, msg: &[u8]) {
    self.hash = u64_prefix(msg);
  }

  #[inline]
  fn finish(&self) -> u64 {
      self.hash
  }
} 



#[derive(Debug, Default)]
pub struct CompressedSecondaryIndex {
  metrics_name: &'static str,
  stats: SecondaryIndexStats,

  pub index: DashMap<Pubkey, DashSet<u64, PrefixHasher>, PrefixHasher>,
  pub reverse_index: DashMap<u64, RwLock<Vec<u64>>, PrefixHasher>,
}

impl CompressedSecondaryIndex {
  pub fn new(metrics_name: &'static str) -> Self {
    Self {
        metrics_name,
        ..Self::default()
    }
  }

  pub fn insert(&self, key: &Pubkey, inner_key: &Pubkey) {

    let inner_key_prefix = u64_prefix(inner_key);
    {
      let prefix_set = self
          .index
          .get(key)
          .unwrap_or_else(|| self.index.entry(*key).or_default().downgrade());

      prefix_set.insert_if_not_exists(inner_key_prefix, &self.stats.num_inner_keys);
    }

    {
      let outer_keys = self.reverse_index.get(&inner_key_prefix).unwrap_or_else(|| {
          self.reverse_index
              .entry(*inner_key_prefix)
              .or_insert(RwLock::new(Vec::with_capacity(1)))
              .downgrade()
      });

      let should_insert = !outer_keys.read().unwrap().contains(key);
      if should_insert {
          let mut w_outer_keys = outer_keys.write().unwrap();
          if !w_outer_keys.contains(key) {
              w_outer_keys.push(*key);
          }
      }
    }

    if self.stats.last_report.should_update(1000) {
      datapoint_info!(
          self.metrics_name,
          ("num_secondary_keys", self.index.len() as i64, i64),
          (
              "num_inner_keys",
              self.stats.num_inner_keys.load(Ordering::Relaxed) as i64,
              i64
          ),
          (
              "num_reverse_index_keys",
              self.reverse_index.len() as i64,
              i64
          ),
      );
    }
  }


  pub fn get(&self, key: &Pubkey) -> Vec<u64> {
    if let Some(prefix_set) = self.index.get(key) {
        prefix_set.keys()
    } else {
        vec![]
    }
}
}
