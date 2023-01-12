use std::cmp::min;

use crate::clique_stage::CliqueStage;

use {
    crate::{broadcast_stage::BroadcastStage, retransmit_stage::RetransmitStage},
    itertools::Itertools,
    lru::LruCache,
    rand::{seq::SliceRandom, Rng, SeedableRng},
    rand_chacha::ChaChaRng,
    solana_gossip::{
        cluster_info::{compute_retransmit_peers, ClusterInfo, DATA_PLANE_FANOUT},
        crds::GossipRoute,
        crds_gossip_pull::CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
        crds_value::{CrdsData, CrdsValue},
        legacy_contact_info::LegacyContactInfo as ContactInfo,
        weighted_shuffle::WeightedShuffle,
    },
    solana_ledger::shred::ShredId,
    solana_runtime::bank::Bank,
    solana_sdk::{
        clock::{Epoch, Slot},
        feature_set,
        pubkey::Pubkey,
        signature::Keypair,
        timing::timestamp,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        any::TypeId,
        cmp::Reverse,
        collections::HashMap,
        iter::repeat_with,
        marker::PhantomData,
        net::SocketAddr,
        ops::Deref,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

pub(crate) const MAX_NUM_TURBINE_HOPS: usize = 4;

#[allow(clippy::large_enum_variant)]
enum NodeId {
    // TVU node obtained through gossip (staked or not).
    ContactInfo(ContactInfo),
    // Staked node with no contact-info in gossip table.
    Pubkey(Pubkey),
}

pub struct Node {
    node: NodeId,
    stake: u64,
}

pub struct ClusterNodes<T> {
    pubkey: Pubkey, // The local node itself.
    // All staked nodes + other known tvu-peers + the node itself;
    // sorted by (stake, pubkey) in descending order.
    nodes: Vec<Node>,
    // Reverse index from nodes pubkey to their index in self.nodes.
    index: HashMap<Pubkey, /*index:*/ usize>,
    weighted_shuffle: WeightedShuffle</*stake:*/ u64>,
    _phantom: PhantomData<T>,
}

type CacheEntry<T> = Option<(/*as of:*/ Instant, Arc<ClusterNodes<T>>)>;

pub struct ClusterNodesCache<T> {
    // Cache entries are wrapped in Arc<Mutex<...>>, so that, when needed, only
    // one thread does the computations to update the entry for the epoch.
    cache: Mutex<LruCache<Epoch, Arc<Mutex<CacheEntry<T>>>>>,
    ttl: Duration, // Time to live.
}

pub struct RetransmitPeers<'a> {
    root_distance: usize, // distance from the root node
    neighbors: Vec<&'a Node>,
    children: Vec<&'a Node>,
    // Maps from tvu/tvu_forwards addresses to the first node
    // in the shuffle with the same address.
    addrs: HashMap<SocketAddr, Pubkey>, // tvu addresses
    frwds: HashMap<SocketAddr, Pubkey>, // tvu_forwards addresses
}

impl Node {
    #[inline]
    fn pubkey(&self) -> Pubkey {
        match &self.node {
            NodeId::Pubkey(pubkey) => *pubkey,
            NodeId::ContactInfo(node) => node.id,
        }
    }

    #[inline]
    fn contact_info(&self) -> Option<&ContactInfo> {
        match &self.node {
            NodeId::Pubkey(_) => None,
            NodeId::ContactInfo(node) => Some(node),
        }
    }
}

impl<T> ClusterNodes<T> {
    pub(crate) fn submit_metrics(&self, name: &'static str, now: u64) {
        let mut num_nodes_dead = 0;
        let mut num_nodes_staked = 0;
        let mut num_nodes_stale = 0;
        for node in &self.nodes {
            if node.stake != 0u64 {
                num_nodes_staked += 1;
            }
            match node.contact_info() {
                None => {
                    num_nodes_dead += 1;
                }
                Some(node) => {
                    let age = now.saturating_sub(node.wallclock);
                    if age > CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS {
                        num_nodes_stale += 1;
                    }
                }
            }
        }
        num_nodes_stale += num_nodes_dead;
        datapoint_info!(
            name,
            ("num_nodes", self.nodes.len(), i64),
            ("num_nodes_dead", num_nodes_dead, i64),
            ("num_nodes_staked", num_nodes_staked, i64),
            ("num_nodes_stale", num_nodes_stale, i64),
        );
    }
}

impl ClusterNodes<BroadcastStage> {
    pub fn new(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Self {
        let nodes = collect_stake_sorted_nodes(cluster_info, stakes);
        new_cluster_nodes(cluster_info.id(), nodes)
    }

    pub(crate) fn get_broadcast_peer(&self, shred: &ShredId) -> Option<&ContactInfo> {
        let shred_seed = shred.seed(&self.pubkey);
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let index = self.weighted_shuffle.first(&mut rng)?;
        self.nodes[index].contact_info()
    }
}

impl ClusterNodes<CliqueStage> {
    pub fn new(self_pubkey: Pubkey, clique_nodes: &Vec<(Pubkey, SocketAddr)>) -> Self {
        // all nodes in clique have the same stake weight
        let stake = 1;

        let nodes = clique_nodes
            .iter()
            .map(|(pk, addr)| {
                let sparse_contact_info = ContactInfo {
                    id: *pk,
                    tvu: *addr,
                    ..Default::default()
                };
                Node {
                    node: NodeId::from(sparse_contact_info),
                    stake,
                }
            })
            .sorted_by_key(|node| Reverse(node.pubkey()))
            // Since sorted_by_key is stable, in case of duplicates, this
            // will keep nodes with contact-info.
            .dedup_by(|a, b| a.pubkey() == b.pubkey())
            .collect();

        new_cluster_nodes(self_pubkey, nodes)
    }

    pub(crate) fn get_retransmit_addrs(
        &self,
        shred: &ShredId,
        fanout: usize,
    ) -> (/*root_distance:*/ usize, Vec<SocketAddr>) {
        let RetransmitPeers {
            root_distance,
            neighbors,
            children,
            addrs,
            frwds: _
        } = self.get_retransmit_peers(shred, fanout);

        // Neigbors can include duplicates
        let peers = neighbors
            .iter()
            .chain(children.iter())
            .filter_map(|n| n.contact_info())
            .filter(|node| addrs.get(&node.tvu) == Some(&node.id))
            .map(|node| node.tvu)
            .dedup()
            .collect();
        (root_distance, peers)
    }

    // this creates a graph that has the following structure & root distances for F=fanout:
    // 0: [], there are no nodes with root distance 0
    // 1: [0..F), all nodes w/ root distance 1 are linked as neighbours
    // 2: [[F..2F),...,[F^2..F^2+F)], all nodes w/ root distance >=2 are linked to their parent [0..F)
    // all neighbour and parent-child links are bi-directional
    // RetransmitPeers.neighbors contains neigbor <-> neigbor and child->parent 
    // RetransmitPeers.children contains parent -> child
    pub fn get_retransmit_peers(&self, shred: &ShredId, fanout: usize) -> RetransmitPeers {
        let shred_seed = shred.seed(&Pubkey::default());
        let weighted_shuffle = self.weighted_shuffle.clone();

        let mut addrs = HashMap::<SocketAddr, Pubkey>::with_capacity(self.nodes.len());
        let frwds = Default::default();
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let nodes: Vec<_> = weighted_shuffle
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .inspect(|node| {
                if let Some(node) = node.contact_info() {
                    addrs.entry(node.tvu).or_insert(node.id);
                }
            })
            .collect();

        if nodes.len() == 0 {
            return RetransmitPeers {
                root_distance: 0usize,
                neighbors: Default::default(),
                children: Default::default(),
                addrs,
                frwds
            };
        }

        let self_index = nodes
            .iter()
            .position(|node| node.pubkey() == self.pubkey)
            .unwrap();

        // children can lie outside of the array range, but skip & take handle that case
        let children_start = (self_index + 1) * fanout;
        let children = nodes
            .iter()
            .skip(children_start)
            .take(fanout)
            .map(|n| *n)
            .collect();

        // calculate root_distance and level_end_index
        let mut root_distance = 1;
        while root_distance < 10 {
            let next_level_start_index = (1..root_distance+1).map(|rd| fanout.pow(rd)).sum();
            if self_index < next_level_start_index {
                break;
            }
            root_distance += 1;
        }

        let neighbors = if root_distance == 1 {
            // top level has no parent relationship, only neighbors
            // ensure neighbor indexes don't lie outside of array range
            let l1_len = min(nodes.len(), fanout);
            let left_neighbor = min(l1_len - 1, self_index + fanout - 1) % fanout;
            let right_neighbor = (self_index + 1) % l1_len;
            if l1_len == 1 {
                // if there's only one node, it doesn't have any neighbors
                vec![]
            } else if l1_len == 2 {
                // left_neighbor == right_neighbor, so we only need to return it once
                vec![nodes[left_neighbor]]
            } else {
                vec![nodes[left_neighbor], nodes[right_neighbor]]
            }
        } else {
            // lower levels only have their parent as neighbor, no range checks needed
            let parent_index = (self_index / fanout) - 1;
            vec![nodes[parent_index]]
        };

        RetransmitPeers {
            root_distance: root_distance as usize,
            neighbors,
            children,
            addrs,
            frwds,
        }
    }
}

impl ClusterNodes<RetransmitStage> {
    pub fn new(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Self {
        let nodes = collect_stake_sorted_nodes(cluster_info, stakes);
        new_cluster_nodes(cluster_info.id(), nodes)
    }
    
    pub(crate) fn get_retransmit_addrs(
        &self,
        slot_leader: &Pubkey,
        shred: &ShredId,
        root_bank: &Bank,
        fanout: usize,
    ) -> (/*root_distance:*/ usize, Vec<SocketAddr>) {
        let RetransmitPeers {
            root_distance,
            neighbors,
            children,
            addrs,
            frwds,
        } = self.get_retransmit_peers(slot_leader, shred, root_bank, fanout);
        if neighbors.is_empty() {
            let peers = children
                .into_iter()
                .filter_map(Node::contact_info)
                .filter(|node| addrs.get(&node.tvu) == Some(&node.id))
                .map(|node| node.tvu)
                .collect();
            return (root_distance, peers);
        }
        // If the node is on the critical path (i.e. the first node in each
        // neighborhood), it should send the packet to tvu socket of its
        // children and also tvu_forward socket of its neighbors. Otherwise it
        // should only forward to tvu_forwards socket of its children.
        if neighbors[0].pubkey() != self.pubkey {
            let peers = children
                .into_iter()
                .filter_map(Node::contact_info)
                .filter(|node| frwds.get(&node.tvu_forwards) == Some(&node.id))
                .map(|node| node.tvu_forwards);
            return (root_distance, peers.collect());
        }
        // First neighbor is this node itself, so skip it.
        let peers = neighbors[1..]
            .iter()
            .filter_map(|node| node.contact_info())
            .filter(|node| frwds.get(&node.tvu_forwards) == Some(&node.id))
            .map(|node| node.tvu_forwards)
            .chain(
                children
                    .into_iter()
                    .filter_map(Node::contact_info)
                    .filter(|node| addrs.get(&node.tvu) == Some(&node.id))
                    .map(|node| node.tvu),
            );
        (root_distance, peers.collect())
    }

    pub fn get_retransmit_peers(
        &self,
        slot_leader: &Pubkey,
        shred: &ShredId,
        root_bank: &Bank,
        fanout: usize,
    ) -> RetransmitPeers {
        let shred_seed = shred.seed(slot_leader);
        let mut weighted_shuffle = self.weighted_shuffle.clone();
        // Exclude slot leader from list of nodes.
        if slot_leader == &self.pubkey {
            error!("retransmit from slot leader: {}", slot_leader);
        } else if let Some(index) = self.index.get(slot_leader) {
            weighted_shuffle.remove_index(*index);
        };
        let mut addrs = HashMap::<SocketAddr, Pubkey>::with_capacity(self.nodes.len());
        let mut frwds = HashMap::<SocketAddr, Pubkey>::with_capacity(self.nodes.len());
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let drop_redundant_turbine_path = drop_redundant_turbine_path(shred.slot(), root_bank);
        let nodes: Vec<_> = weighted_shuffle
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .inspect(|node| {
                if let Some(node) = node.contact_info() {
                    addrs.entry(node.tvu).or_insert(node.id);
                    if !drop_redundant_turbine_path {
                        frwds.entry(node.tvu_forwards).or_insert(node.id);
                    }
                }
            })
            .collect();
        let self_index = nodes
            .iter()
            .position(|node| node.pubkey() == self.pubkey)
            .unwrap();
        if drop_redundant_turbine_path {
            let root_distance = if self_index == 0 {
                0
            } else if self_index <= fanout {
                1
            } else if self_index <= fanout.saturating_add(1).saturating_mul(fanout) {
                2
            } else {
                3 // If changed, update MAX_NUM_TURBINE_HOPS.
            };
            let peers = get_retransmit_peers(fanout, self_index, &nodes);
            return RetransmitPeers {
                root_distance,
                neighbors: Vec::default(),
                children: peers.collect(),
                addrs,
                frwds,
            };
        }
        let root_distance = if self_index == 0 {
            0
        } else if self_index < fanout {
            1
        } else if self_index < fanout.saturating_add(1).saturating_mul(fanout) {
            2
        } else {
            3 // If changed, update MAX_NUM_TURBINE_HOPS.
        };
        let (neighbors, children) = compute_retransmit_peers(fanout, self_index, &nodes);
        // Assert that the node itself is included in the set of neighbors, at
        // the right offset.
        debug_assert_eq!(neighbors[self_index % fanout].pubkey(), self.pubkey);
        RetransmitPeers {
            root_distance,
            neighbors,
            children,
            addrs,
            frwds,
        }
    }
}

// nodes is expected to be sorted by (stake, pubkey) in descending order
pub fn new_cluster_nodes<T: 'static>(self_pubkey: Pubkey, nodes: Vec<Node>) -> ClusterNodes<T> {
    let index: HashMap<_, _> = nodes
        .iter()
        .enumerate()
        .map(|(ix, node)| (node.pubkey(), ix))
        .collect();
    let broadcast = TypeId::of::<T>() == TypeId::of::<BroadcastStage>();
    let stakes: Vec<u64> = nodes.iter().map(|node| node.stake).collect();
    let mut weighted_shuffle = WeightedShuffle::new("cluster-nodes", &stakes);
    if broadcast {
        weighted_shuffle.remove_index(index[&self_pubkey]);
    }
    ClusterNodes {
        pubkey: self_pubkey,
        nodes,
        index,
        weighted_shuffle,
        _phantom: PhantomData::default(),
    }
}

// All staked nodes + other known tvu-peers + the node itself;
// sorted by (stake, pubkey) in descending order.
fn collect_stake_sorted_nodes(
    cluster_info: &ClusterInfo,
    stakes: &HashMap<Pubkey, u64>,
) -> Vec<Node> {
    let self_pubkey = cluster_info.id();

    // The local node itself.
    std::iter::once({
        let stake = stakes.get(&self_pubkey).copied().unwrap_or_default();
        let node = NodeId::from(cluster_info.my_contact_info());
        Node { node, stake }
    })
    // All known tvu-peers from gossip.
    .chain(cluster_info.tvu_peers().into_iter().map(|node| {
        let stake = stakes.get(&node.id).copied().unwrap_or_default();
        let node = NodeId::from(node);
        Node { node, stake }
    }))
    // All staked nodes.
    .chain(
        stakes
            .iter()
            .filter(|(_, stake)| **stake > 0)
            .map(|(&pubkey, &stake)| Node {
                node: NodeId::from(pubkey),
                stake,
            }),
    )
    .sorted_by_key(|node| Reverse((node.stake, node.pubkey())))
    // Since sorted_by_key is stable, in case of duplicates, this
    // will keep nodes with contact-info.
    .dedup_by(|a, b| a.pubkey() == b.pubkey())
    .collect()
}

// root     : [0]
// 1st layer: [1, 2, ..., fanout]
// 2nd layer: [[fanout + 1, ..., fanout * 2],
//             [fanout * 2 + 1, ..., fanout * 3],
//             ...
//             [fanout * fanout + 1, ..., fanout * (fanout + 1)]]
// 3rd layer: ...
// ...
// The leader node broadcasts shreds to the root node.
// The root node retransmits the shreds to all nodes in the 1st layer.
// Each other node retransmits shreds to fanout many nodes in the next layer.
// For example the node k in the 1st layer will retransmit to nodes:
// fanout + k, 2*fanout + k, ..., fanout*fanout + k
fn get_retransmit_peers<T: Copy>(
    fanout: usize,
    index: usize, // Local node's index withing the nodes slice.
    nodes: &[T],
) -> impl Iterator<Item = T> + '_ {
    // Node's index within its neighborhood.
    let offset = index.saturating_sub(1) % fanout;
    // First node in the neighborhood.
    let anchor = index - offset;
    let step = if index == 0 { 1 } else { fanout };
    (anchor * fanout + offset + 1..)
        .step_by(step)
        .take(fanout)
        .map(|i| nodes.get(i))
        .while_some()
        .copied()
}

impl<T> ClusterNodesCache<T> {
    pub fn new(
        // Capacity of underlying LRU-cache in terms of number of epochs.
        cap: usize,
        // A time-to-live eviction policy is enforced to refresh entries in
        // case gossip contact-infos are updated.
        ttl: Duration,
    ) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            ttl,
        }
    }
}

impl<T: 'static> ClusterNodesCache<T> {
    fn get_cache_entry(&self, epoch: Epoch) -> Arc<Mutex<CacheEntry<T>>> {
        let mut cache = self.cache.lock().unwrap();
        match cache.get(&epoch) {
            Some(entry) => Arc::clone(entry),
            None => {
                let entry = Arc::default();
                cache.put(epoch, Arc::clone(&entry));
                entry
            }
        }
    }

    pub(crate) fn get(
        &self,
        shred_slot: Slot,
        root_bank: &Bank,
        working_bank: &Bank,
        cluster_info: &ClusterInfo,
    ) -> Arc<ClusterNodes<T>> {
        let epoch = root_bank.get_leader_schedule_epoch(shred_slot);
        let entry = self.get_cache_entry(epoch);
        // Hold the lock on the entry here so that, if needed, only
        // one thread recomputes cluster-nodes for this epoch.
        let mut entry = entry.lock().unwrap();
        if let Some((asof, nodes)) = entry.deref() {
            if asof.elapsed() < self.ttl {
                return Arc::clone(nodes);
            }
        }
        let epoch_staked_nodes = [root_bank, working_bank]
            .iter()
            .find_map(|bank| bank.epoch_staked_nodes(epoch));
        if epoch_staked_nodes.is_none() {
            inc_new_counter_info!("cluster_nodes-unknown_epoch_staked_nodes", 1);
            if epoch != root_bank.get_leader_schedule_epoch(root_bank.slot()) {
                return self.get(root_bank.slot(), root_bank, working_bank, cluster_info);
            }
            inc_new_counter_info!("cluster_nodes-unknown_epoch_staked_nodes_root", 1);
        }
        let nodes =
            collect_stake_sorted_nodes(cluster_info, &epoch_staked_nodes.unwrap_or_default());
        let nodes = Arc::new(new_cluster_nodes::<T>(cluster_info.id(), nodes));
        *entry = Some((Instant::now(), Arc::clone(&nodes)));
        nodes
    }
}

impl From<ContactInfo> for NodeId {
    fn from(node: ContactInfo) -> Self {
        NodeId::ContactInfo(node)
    }
}

impl From<Pubkey> for NodeId {
    fn from(pubkey: Pubkey) -> Self {
        NodeId::Pubkey(pubkey)
    }
}

pub fn make_test_cluster<R: Rng>(
    rng: &mut R,
    num_nodes: usize,
    unstaked_ratio: Option<(u32, u32)>,
) -> (
    Vec<ContactInfo>,
    HashMap<Pubkey, u64>, // stakes
    ClusterInfo,
) {
    let (unstaked_numerator, unstaked_denominator) = unstaked_ratio.unwrap_or((1, 7));
    let mut nodes: Vec<_> = repeat_with(|| ContactInfo::new_rand(rng, None))
        .take(num_nodes)
        .collect();
    nodes.shuffle(rng);
    let this_node = nodes[0].clone();
    let mut stakes: HashMap<Pubkey, u64> = nodes
        .iter()
        .filter_map(|node| {
            if rng.gen_ratio(unstaked_numerator, unstaked_denominator) {
                None // No stake for some of the nodes.
            } else {
                Some((node.id, rng.gen_range(0, 20)))
            }
        })
        .collect();
    // Add some staked nodes with no contact-info.
    stakes.extend(repeat_with(|| (Pubkey::new_unique(), rng.gen_range(0, 20))).take(100));
    let cluster_info = ClusterInfo::new(
        this_node,
        Arc::new(Keypair::new()),
        SocketAddrSpace::Unspecified,
    );
    {
        let now = timestamp();
        let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
        // First node is pushed to crds table by ClusterInfo constructor.
        for node in nodes.iter().skip(1) {
            let node = CrdsData::LegacyContactInfo(node.clone());
            let node = CrdsValue::new_unsigned(node);
            assert_eq!(
                gossip_crds.insert(node, now, GossipRoute::LocalMessage),
                Ok(())
            );
        }
    }
    (nodes, stakes, cluster_info)
}

pub(crate) fn get_data_plane_fanout(shred_slot: Slot, root_bank: &Bank) -> usize {
    if enable_turbine_fanout_experiments(shred_slot, root_bank) {
        // Allocate ~2% of slots to turbine fanout experiments.
        match shred_slot % 359 {
            11 => 64,
            61 => 768,
            111 => 128,
            161 => 640,
            211 => 256,
            261 => 512,
            311 => 384,
            _ => DATA_PLANE_FANOUT,
        }
    } else {
        DATA_PLANE_FANOUT
    }
}

fn drop_redundant_turbine_path(shred_slot: Slot, root_bank: &Bank) -> bool {
    check_feature_activation(
        &feature_set::drop_redundant_turbine_path::id(),
        shred_slot,
        root_bank,
    )
}

fn enable_turbine_fanout_experiments(shred_slot: Slot, root_bank: &Bank) -> bool {
    check_feature_activation(
        &feature_set::enable_turbine_fanout_experiments::id(),
        shred_slot,
        root_bank,
    ) && !check_feature_activation(
        &feature_set::disable_turbine_fanout_experiments::id(),
        shred_slot,
        root_bank,
    )
}

// Returns true if the feature is effective for the shred slot.
fn check_feature_activation(feature: &Pubkey, shred_slot: Slot, root_bank: &Bank) -> bool {
    match root_bank.feature_set.activated_slot(feature) {
        None => false,
        Some(feature_slot) => {
            let epoch_schedule = root_bank.epoch_schedule();
            let feature_epoch = epoch_schedule.get_epoch(feature_slot);
            let shred_epoch = epoch_schedule.get_epoch(shred_slot);
            feature_epoch < shred_epoch
        }
    }
}

#[cfg(test)]
mod tests {
    use solana_ledger::shred::layout::get_shred_id;

    use super::*;

    #[test]
    fn test_cluster_nodes_retransmit() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_test_cluster(&mut rng, 1_000, None);
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = ClusterNodes::<RetransmitStage>::new(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
    }

    #[test]
    fn test_cluster_nodes_broadcast() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_test_cluster(&mut rng, 1_000, None);
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = ClusterNodes::<BroadcastStage>::new(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        // Excluding this node itself.
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
    }

    #[test]
    fn test_get_retransmit_peers() {
        // fanout 2
        let index = vec![
            7, // root
            6, 10, // 1st layer
            // 2nd layer
            5, 19, // 1st neighborhood
            0, 14, // 2nd
            // 3rd layer
            3, 1, // 1st neighborhood
            12, 2, // 2nd
            11, 4, // 3rd
            15, 18, // 4th
            // 4th layer
            13, 16, // 1st neighborhood
            17, 9, // 2nd
            8, // 3rd
        ];
        let peers = vec![
            vec![6, 10],
            vec![5, 0],
            vec![19, 14],
            vec![3, 12],
            vec![1, 2],
            vec![11, 15],
            vec![4, 18],
            vec![13, 17],
            vec![16, 9],
            vec![8],
        ];
        for (k, peers) in peers.into_iter().enumerate() {
            let retransmit_peers = get_retransmit_peers(/*fanout:*/ 2, k, &index);
            assert_eq!(retransmit_peers.collect::<Vec<_>>(), peers);
        }
        for k in 10..=index.len() {
            let mut retransmit_peers = get_retransmit_peers(/*fanout:*/ 2, k, &index);
            assert_eq!(retransmit_peers.next(), None);
        }
        // fanout 3
        let index = vec![
            19, // root
            14, 15, 28, // 1st layer
            // 2nd layer
            29, 4, 5, // 1st neighborhood
            9, 16, 7, // 2nd
            26, 23, 2, // 3rd
            // 3rd layer
            31, 3, 17, // 1st neighborhood
            20, 25, 0, // 2nd
            13, 30, 18, // 3rd
            35, 21, 22, // 4th
            6, 8, 11, // 5th
            27, 1, 10, // 6th
            12, 24, 34, // 7th
            33, 32, // 8th
        ];
        let peers = vec![
            vec![14, 15, 28],
            vec![29, 9, 26],
            vec![4, 16, 23],
            vec![5, 7, 2],
            vec![31, 20, 13],
            vec![3, 25, 30],
            vec![17, 0, 18],
            vec![35, 6, 27],
            vec![21, 8, 1],
            vec![22, 11, 10],
            vec![12, 33],
            vec![24, 32],
            vec![34],
        ];
        for (k, peers) in peers.into_iter().enumerate() {
            let retransmit_peers = get_retransmit_peers(/*fanout:*/ 3, k, &index);
            assert_eq!(retransmit_peers.collect::<Vec<_>>(), peers);
        }
        for k in 13..=index.len() {
            let mut retransmit_peers = get_retransmit_peers(/*fanout:*/ 3, k, &index);
            assert_eq!(retransmit_peers.next(), None);
        }
    }


    fn generate_cluster_nodes(num: usize) -> Vec<ClusterNodes<CliqueStage>> {
        let keys: Vec<Pubkey> = (0..num).map(|i| Pubkey::find_program_address(&[&[(i / 256) as u8, (i % 256) as u8]], &Pubkey::default()).0).collect();
        let addrs: Vec<SocketAddr> = (0..num).map(|i| format!("1.2.3.4:{}", 13000+i).parse().unwrap()).collect();
        let clique = (0..num).map(|i| (keys[i], addrs[i])).collect();
        return (0..num).map(|i| ClusterNodes::<CliqueStage>::new(keys[i], &clique)).collect();
    }

    #[test]
    fn test_get_clique_peers_solo() {
        let single_node_cluster = generate_cluster_nodes(1);
        let shred_id = get_shred_id(&(0..83).collect::<Vec<u8>>().as_slice()).unwrap();
        let peers = single_node_cluster[0].get_retransmit_peers(&shred_id, 2);
        assert_eq!(peers.root_distance, 1);
        assert_eq!(peers.neighbors.len(), 0);
        assert_eq!(peers.children.len(), 0);
    }

    #[test]
    fn test_get_clique_peers_neighbors() {
        let dual_node_cluster = generate_cluster_nodes(4);
        let shred_id = get_shred_id(&(0..83).collect::<Vec<u8>>().as_slice()).unwrap();

        // L1: [0: n=1,3] [1: n=0,2] [2: n=1,3] [3: n=0,2]
        for peers in dual_node_cluster.iter().map(|ns| ns.get_retransmit_peers(&shred_id, 4)) {
            assert_eq!(peers.root_distance, 1);
            assert_eq!(peers.neighbors.len(), 2);
            assert_eq!(peers.children.len(), 0);
        }
    }


    #[test]
    fn test_get_clique_peers_children() {
        let deep_cluster = generate_cluster_nodes(13);
        let shred_id = get_shred_id(&(0..83).collect::<Vec<u8>>().as_slice()).unwrap();
        let peers: Vec<_> = deep_cluster.iter().map(|ns| ns.get_retransmit_peers(&shred_id, 3)).collect();

        // L1: [0: n=1,2 c=4,5,6] [1: n=0,2 c=7,8,9] [2: n=0,1 c=10,11,12]
        let l1: Vec<_> = peers.iter().filter(|ps| ps.root_distance == 1).collect();
        assert_eq!(l1.len(), 3);
        for peers in l1 {
            assert_eq!(peers.neighbors.len(), 2);
            assert_eq!(peers.children.len(), 3);
        }

        // L2: [4: n=0 c=12], [5: n=0], ..., [11: n=2]
        let l2: Vec<_> = peers.iter().filter(|ps| ps.root_distance == 2).sorted_by_key(|ps| -1 * ps.children.len() as i64).collect();
        assert_eq!(l2.len(), 9);
        for (i, peers) in l2.iter().enumerate() {
            assert_eq!(peers.neighbors.len(), 1);
            if i == 0 {
                assert_eq!(peers.children.len(), 1);
             } else {
                assert_eq!(peers.children.len(), 0);
             }
        }

        // L3: [12: n=4]
        let l3: Vec<_> = peers.iter().filter(|ps| ps.root_distance == 3).collect();
        assert_eq!(l3.len(), 1);
        for peers in l3 {
            assert_eq!(peers.neighbors.len(), 1);
            assert_eq!(peers.children.len(), 0);
        }

    }
}
