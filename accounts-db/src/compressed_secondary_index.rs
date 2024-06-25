use std::{fmt, hash::Hasher, mem::transmute, ops::RangeBounds};
use std::fmt::{Debug, Formatter};
use std::ops::{Bound, RangeFull, RangeInclusive};
use std::sync::atomic::{AtomicU64, Ordering};

use bv::BitsExt;
use dashmap::{lock::RwLock, DashMap, DashSet};
use solana_sdk::pubkey::{self, Pubkey};
use solana_sdk::timing::AtomicInterval;
use crate::secondary_index::SecondaryIndexStats;

pub (crate) fn u64_prefix_pubkey(pubkey: &Pubkey) -> u64 {
    let bytes = pubkey.as_ref();
    u64_prefix_raw(bytes)
}

// TODO bench+improve; check if 0..8 is significant
pub (crate) fn u64_prefix_raw(msg: &[u8]) -> u64 {
  if msg.len() >= 8 {
    u64::from_ne_bytes(*msg[0..8])
  } else {
    let mut pad = [0u8; 8];
    pad[0..msg.len()].copy_from_slice(msg);
    u64::from_ne_bytes(pad)
  }
}

// smallest pubkey in group
pub (crate) fn prefix_to_pubkey_smallest(p: u64) -> Pubkey {
  let mut bytes = [0u8; 32];
  bytes[0..8].copy_from_slice(&p.to_ne_bytes());
  Pubkey::new_from_array(bytes)
}

pub (crate) fn prefix_to_pubkey_largest_in_group(p: u64) -> Pubkey {
    let mut bytes = [255u8; 32];
    bytes[0..8].copy_from_slice(&p.to_ne_bytes());
    Pubkey::new_from_array(bytes)
}

pub(crate) fn prefix_to_bound(p: u64) -> RangeInclusive<Pubkey> {
    let begin_incl = prefix_to_pubkey_smallest(p);
    let end_incl = if p == u64::MAX {
        const MAX_PUBKEY: Pubkey = Pubkey::from([255u8; 32]);
        MAX_PUBKEY
        // // FIXME that looks wrong
        // Bound::Included(Pubkey::from([1u8; 32]))
    } else {
        prefix_to_pubkey_largest_in_group(p)
    };
    (begin_incl..=end_incl)
}

// TODO: finish implementation
// TODO check if range should be right-open or right-closed
pub (crate) fn group_prefixes(prefix_set: &mut Vec<u64>, group_size_bits: u8) -> Vec<(RangeInclusive<Pubkey>, Vec(u64))> {

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
      let range = (Bound::Included(start), Bound::Included(end.unwrap_or(u64::MAX)));
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
    self.hash = u64_prefix_raw(msg);
  }

  #[inline]
  fn finish(&self) -> u64 {
      self.hash
  }
}



// inner key = account pubkey
// (outer) key = owner key / mint key
pub struct CompressedSecondaryIndex {
  metrics_name: &'static str,
  stats: SecondaryIndexStats,

    // outer key -> prefixes of inner keys
    pub index: DashMap<Pubkey, DashSet<u64, PrefixHasher>, PrefixHasher>,
    // inner key -> prefixes of outer keys
  // pub reverse_index: DashMap<u64, RwLock<Vec<u64>>, PrefixHasher>,
}

impl Debug for CompressedSecondaryIndex {
  fn fmt(&self, f: &mut Formatter) -> fmt::Result {
    f.debug_struct("CompressedSecondaryIndex")
      .field("metrics_name", &self.metrics_name)
      .field("stats", &self.stats)
      .field("index.len", &self.index.len())
      .finish()
  }
}

impl CompressedSecondaryIndex {
  pub fn new(metrics_name: &'static str) -> Self {
      Self {
          metrics_name,
          stats: SecondaryIndexStats::default(),
          index: DashMap::with_capacity_and_hasher(10000, PrefixHasher::default()),
          // reverse_index: DashMap::new(),
      }
  }

  pub fn insert(&self, key: &Pubkey, inner_key: &Pubkey) {

    let inner_key_prefix = u64_prefix_pubkey(inner_key);
    {
        let prefix_set_lock = self.index.entry(*key).or_default();

        let inserted = prefix_set_lock.insert(inner_key_prefix);
        if inserted {
            self.stats.num_inner_keys.fetch_add(1, Ordering::Relaxed);
        }
        // prefix_set_lock.insert_if_not_exists(inner_key_prefix, &self.stats.num_inner_keys);
    }

      // TODO implement
    // {
    //   let outer_keys = self.reverse_index.get(&inner_key_prefix).unwrap_or_else(|| {
    //       self.reverse_index
    //           .entry(*inner_key_prefix)
    //           .or_insert(RwLock::new(Vec::with_capacity(1)))
    //           .downgrade()
    //   });
    //
    //   let should_insert = !outer_keys.read().unwrap().contains(key);
    //   if should_insert {
    //       let mut w_outer_keys = outer_keys.write().unwrap();
    //       if !w_outer_keys.contains(key) {
    //           w_outer_keys.push(*key);
    //       }
    //   }
    // }

    if self.stats.last_report.should_update(1000) {
      datapoint_info!(
          self.metrics_name,
          ("num_secondary_keys", self.index.len() as i64, i64),
          (
              "num_inner_keys",
              self.stats.num_inner_keys.load(Ordering::Relaxed) as i64,
              i64
          ),
          // (
          //     "num_reverse_index_keys",
          //     self.reverse_index.len() as i64,
          //     i64
          // ),
      );
    }
  }


  pub fn get(&self, key: &Pubkey) -> Vec<u64> {
    if let Some(prefix_set) = self.index.get(key) {
        prefix_set.iter().map(|x| *x).collect()
    } else {
        vec![]
    }
}
}
