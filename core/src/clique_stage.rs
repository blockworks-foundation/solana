use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    convert::TryFrom,
    hash::{Hash, Hasher},
    net::{UdpSocket, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    task::{Context, Poll},
    thread::{Builder, JoinHandle, self},
    time::{Duration, Instant}, iter::{repeat, once},
};

use crossbeam_channel::Receiver;
use futures::task::noop_waker;
use futures_lite::stream::StreamExt;
use itertools::{Itertools, izip};
use libp2p::{
    core, gossipsub, identify, identity, noise, ping, swarm::NetworkBehaviour, swarm::SwarmEvent,
    tcp, yamux, Multiaddr, PeerId, Swarm, Transport, multiaddr::Protocol,
};
use lru::LruCache;
use rayon::{ThreadPoolBuilder, prelude::{IntoParallelIterator, ParallelIterator}};
use solana_gossip::legacy_contact_info::LegacyContactInfo;
use solana_ledger::{shred::{ShredId, layout::get_shred_id}};
use solana_measure::measure::Measure;
use solana_rayon_threadlimit::get_thread_count;
use solana_sdk::{pubkey::Pubkey, signer::Signer};
use solana_streamer::{socket::SocketAddrSpace, sendmmsg::{SendPktsError, multi_target_send}};

use crate::{cluster_nodes::ClusterNodes, packet_hasher::PacketHasher, retransmit_stage::{maybe_reset_shreds_received_cache, should_skip_retransmit}};


const RECV_TIMEOUT: Duration = Duration::from_secs(1);
const DATA_PLANE_FANOUT: usize = 8;
const DEFAULT_LRU_SIZE: usize = 10_000;
// Minimum number of shreds to use rayon parallel iterators.
const PAR_ITER_MIN_NUM_SHREDS: usize = 2;
const METRICS_SUBMIT_CADENCE: Duration = Duration::from_secs(2);
const GOSSIP_HEARTBEAT_CADENCE: Duration = Duration::from_secs(10);
 // allow for at least a second heartbeat message to arrive
const CLIQUE_WARMUP_SLOTS: u64 = 30;
 // allow for at least three heartbeat messages to be missed
const CLIQUE_TIMEOUT_SLOTS: u64 = 70;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
enum CliqueGossipMessage {
    Heartbeat(CliqueHeartbeatMessage)
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct CliqueHeartbeatMessage {
    boot_slot: u64,
    current_slot: u64,
    shred_inbound: SocketAddr,
}


struct CliqueStageStats {
    since: Instant,
    batches_received: usize,
    shreds_received: usize,
    shreds_skipped: usize,
    shreds_outbound: usize,
    transmit_plan_ns: u64,
    transmit_attempts: usize,
    transmit_errors: usize,
    transmit_execute_ns: u64,
}

impl CliqueStageStats {

    fn new() -> Self {
        Self {
            since: Instant::now(),
            batches_received: 0usize,
            shreds_received: 0usize,
            shreds_skipped: 0usize,
            shreds_outbound: 0usize,
            transmit_plan_ns: 0,
            transmit_attempts: 0usize,
            transmit_errors: 0usize,
            transmit_execute_ns: 0,
        }
    }

    fn maybe_submit(&mut self) {
        if self.since.elapsed() <= METRICS_SUBMIT_CADENCE {
            return;
        }
        datapoint_info!(
            "clique_stage",
            ("batches_received", self.batches_received, i64),
            ("shreds_received", self.shreds_received, i64),
            ("shreds_skipped", self.shreds_skipped, i64),
            ("shreds_outbound", self.shreds_outbound, i64),
            ("transmit_plan_ns", self.transmit_plan_ns, i64),
            ("transmit_attempts", self.transmit_attempts, i64),
            ("transmit_errors", self.transmit_errors, i64),
            ("transmit_execute_ns", self.transmit_execute_ns, i64),
        );
        *self = Self::new();
    }
}

#[derive(NetworkBehaviour)]
struct SolanaCliqueBehaviour {
    gossipsub: gossipsub::Gossipsub,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

pub struct CliqueStage {
    gossip_thread_hdl: JoinHandle<()>,
    outbound_thread_hdl: JoinHandle<()>,
}

impl CliqueStage {
    pub fn new<S>(
        bind_addr: SocketAddr,
        bootstrap_peers: Arc<Vec<SocketAddr>>,
        clique_outbound_receiver: Receiver<Vec</*shred:*/ Vec<u8>>>,
        clique_outbound_sockets: Arc<Vec<UdpSocket>>,
        exit: Arc<AtomicBool>,
        identity_keypair: Arc<solana_sdk::signature::Keypair>,
        shred_inbound: SocketAddr,
        socket_addr_space: SocketAddrSpace,
        slot_query: S
    ) -> Self where S: 'static + Send + Fn() -> u64 {
        let mut stats = CliqueStageStats::new();
        
        let boot_slot = slot_query();
        let clique_status = Arc::new(RwLock::new(HashMap::new()));
        
        let gossip_clique_status = clique_status.clone();
        let gossip_exit= exit.clone();
        let gossip_identity_keypair = identity_keypair.clone();
        let gossip_shred_inbound = shred_inbound.clone();
        let gossip_thread_hdl = Builder::new()
            .name("solCliqueGossip".to_string())
            .spawn(move || {

                // Derive peer id from solana keypair
                let copy = gossip_identity_keypair.secret().as_bytes().clone();
                let secret_key = identity::ed25519::SecretKey::from_bytes(copy)
                    .expect("CliqueStage solana_keypair is ed25519 compatible");
                let local_key = identity::Keypair::Ed25519(secret_key.into());
                let local_peer_id = PeerId::from(local_key.public());
                debug!("identity {:?}", local_key.public());

                // Set up an encrypted DNS-enabled TCP Transport over the Mplex protocol.
                let transport = tcp::async_io::Transport::default()
                    .upgrade(core::upgrade::Version::V1)
                    .authenticate(
                        noise::NoiseAuthenticated::xx(&local_key.clone())
                            .expect("CliqueStage noise authentication available"),
                    )
                    .multiplex(yamux::YamuxConfig::default())
                    .boxed();


                // To content-address message, we can take the hash of message and use it as an ID.
                let message_id_fn = |message: &gossipsub::GossipsubMessage| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    let hash = s.finish().to_string();
                    gossipsub::MessageId::from(hash)
                };

                // Set a custom gossipsub configuration
                let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
                    .heartbeat_interval(GOSSIP_HEARTBEAT_CADENCE) // This is set to aid debugging by not cluttering the log space
                    .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                    .message_id_fn(message_id_fn) 
                    .build()
                    .expect("CliqueStage valid gossipsub_config");

                // Build a gossipsub network behaviour
                let mut gossipsub = gossipsub::Gossipsub::new(
                    gossipsub::MessageAuthenticity::Signed(local_key.clone()),
                    gossipsub_config,
                )
                .expect("CliqueStage correct gossipsub_config");

                // Create a Gossipsub topic
                let topic: gossipsub::Topic<gossipsub::topic::Sha256Hash> =
                    gossipsub::Topic::new("solana-clique");

                // subscribes to our topic
                gossipsub
                    .subscribe(&topic)
                    .expect("CliqueStage subscribe to topic");

                // Build an identify network behaviour
                let identify = identify::Behaviour::new(identify::Config::new(
                    format!("/solana/{}", solana_version::version!()),
                    local_key.public(),
                ));

                // Build a ping network behaviour
                let ping = ping::Behaviour::new(ping::Config::new());

                // Create a Swarm to manage peers and events from behaviours
                let mut swarm = {
                    let behaviour = SolanaCliqueBehaviour {
                        gossipsub,
                        identify,
                        ping,
                    };
                    Swarm::with_async_std_executor(transport, behaviour, local_peer_id)
                };

                // Reach out to other nodes if specified
                for peer in bootstrap_peers.iter() {
                    let addr = socket_addr_to_multi_addr(*peer);
                    swarm.dial(addr.clone()).expect("dial succeeds");
                    debug!("dial peer: {peer:?} {addr:?}");
                }

                // Listen on all interfaces
                let listen_addr = socket_addr_to_multi_addr(bind_addr);
                swarm.listen_on(listen_addr.clone()).expect(&format!("listen  succeeds {bind_addr:?} {listen_addr:?}"));

                // build a minimal context to execute libp2p's futures
                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                let mut last_heartbeat = Instant::now();
                
                loop {
                    if gossip_exit.load(Ordering::Relaxed) {
                        break;
                    }

                    // broadcast heartbeat message to announce membership of the clique
                    if last_heartbeat.elapsed() > GOSSIP_HEARTBEAT_CADENCE {   
                        let current_slot = slot_query();
                        let message = CliqueHeartbeatMessage {
                            boot_slot, current_slot, shred_inbound: gossip_shred_inbound
                        };

                        debug!("publish: {message:?}");
                        if let Err(e) = swarm
                            .behaviour_mut().gossipsub
                            .publish(topic.clone(), bincode::serialize(&CliqueGossipMessage::Heartbeat(message)).unwrap()) {
                            debug!("publish error: {e:?}");
                        }

                        last_heartbeat = Instant::now();    
                    }

                    if let Poll::Ready(Some(inbound)) = swarm.poll_next(&mut cx) {
                        match inbound {
                            SwarmEvent::NewListenAddr { address, .. } => {
                                debug!("listen on {address:?}");
                            }
                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Identify(event)) => {
                                debug!("identify: {event:?}");
                            }
                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Gossipsub(event)) => {
                                match event {
                                    gossipsub::GossipsubEvent::Message {
                                        propagation_source,
                                        message_id,
                                        message,
                                    } => {
                                        if let Ok(message) = bincode::deserialize::<CliqueGossipMessage>(&message.data.as_slice()) {
                                            let peer_pk = peer_id_to_solana_pubkey(propagation_source);
                                            debug!("gossipsub: inbound peer={} id={:?} message={:?}", peer_pk.to_string(), message_id, message);

                                            match message {
                                                CliqueGossipMessage::Heartbeat(message) => {
                                                    gossip_clique_status.write().unwrap().insert(peer_pk, message);
                                                }
                                            }
                                        }
                                    }
                                    gossipsub::GossipsubEvent::Subscribed { peer_id, topic } => {
                                        let peer_pk = peer_id_to_solana_pubkey(peer_id);
                                        debug!("gossipsub: peer={} subscribed topic={}", peer_pk, topic)
                                    }
                                    gossipsub::GossipsubEvent::Unsubscribed { peer_id, topic } => {
                                        let peer_pk = peer_id_to_solana_pubkey(peer_id);
                                        debug!("gossipsub: peer={} unsubscribed topic={}", peer_pk, topic)
                                    }
                                    gossipsub::GossipsubEvent::GossipsubNotSupported { peer_id } => {
                                        let peer_pk = peer_id_to_solana_pubkey(peer_id);
                                        debug!("gossipsub: peer={} unsupported", peer_pk)
                                    }
                                }
                            }

                            SwarmEvent::Behaviour(SolanaCliqueBehaviourEvent::Ping(event)) => {
                                match event {
                                    ping::Event {
                                        peer,
                                        result: Result::Ok(ping::Success::Ping { rtt }),
                                    } => {
                                        debug!(
                                            "ping: rtt to {} is {} ms",
                                            peer.to_base58(),
                                            rtt.as_millis()
                                        );
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Ok(ping::Success::Pong),
                                    } => {
                                        debug!("ping: pong from {}", peer.to_base58());
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Timeout),
                                    } => {
                                        debug!("ping: timeout to {}", peer.to_base58());
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Unsupported),
                                    } => {
                                        debug!(
                                            "ping: {} does not support ping protocol",
                                            peer.to_base58()
                                        );
                                    }
                                    ping::Event {
                                        peer,
                                        result: Result::Err(ping::Failure::Other { error }),
                                    } => {
                                        debug!(
                                            "ping: failure with {}: {error}",
                                            peer.to_base58()
                                        );
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            })
            .unwrap();

        let mut hasher_reset_ts = Instant::now();
        let mut shreds_received = LruCache::<ShredId, _>::new(DEFAULT_LRU_SIZE);
        let mut packet_hasher = PacketHasher::default();
        let num_threads = get_thread_count().min(8).max(clique_outbound_sockets.len());
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("solCliqueRetransmitSender{i:02}"))
            .build()
            .unwrap();

        let outbound_thread_hdl = Builder::new()
            .name("solCliqueOutboundReceiver".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {                
                    if let Ok(mut shreds) = clique_outbound_receiver.recv_timeout(RECV_TIMEOUT) {
                        // collect more shreds that might be available to be transmitted
                        shreds.extend(clique_outbound_receiver.try_iter().flatten());
                        stats.batches_received += 1;
                        stats.shreds_received += shreds.len();
                        
                        maybe_reset_shreds_received_cache(
                            &mut shreds_received,
                            &mut packet_hasher,
                            &mut hasher_reset_ts,
                        );

                        // Skip shreds that have already been transmitted to this clique
                        // Collect all information needed for turbine to determine where to send a shred
                        let mut transmit_plan = Measure::start("clique_transmit_plan");
                        let shreds: Vec<_> = shreds
                            .into_iter()
                            .filter_map(|shred| {
                                let key = get_shred_id(&shred)?;
                                if should_skip_retransmit(key, &shred, &mut shreds_received, &packet_hasher) {
                                    stats.shreds_skipped += 1;
                                    None
                                } else {
                                    Some((key, shred))
                                }
                            })
                            .into_group_map_by(|(key, _)| key.slot())
                            // TODO: evaluate if par_iter can help here, we might send shreds for multiple slots at once
                            .into_iter()
                            .filter_map(|(slot, shreds)| {
                                let active_clique_members: Vec<_> = clique_status
                                    .read()
                                    .unwrap()
                                    .iter()
                                    .filter(|(_, m)| m.boot_slot + CLIQUE_WARMUP_SLOTS < slot && slot < m.current_slot + CLIQUE_TIMEOUT_SLOTS )
                                    .map(|(pk,m)|(*pk, m.shred_inbound))
                                    .chain(once((identity_keypair.pubkey(), shred_inbound)))
                                    .collect();
                                let cluster_nodes = Arc::new(ClusterNodes::<CliqueStage>::new(identity_keypair.pubkey(), &active_clique_members));
                            
                                Some(izip!(shreds, repeat(cluster_nodes)))
                            })
                            .flatten()
                            .collect();
                        transmit_plan.stop();
                        stats.transmit_plan_ns += transmit_plan.as_ns();
                        stats.shreds_outbound += shreds.len();

                        // transmit shreds over UDP (in parallel if needed) while aggregating stats
                        let mut transmit_execute = Measure::start("clique_transmit_execute");
                        let init_stats = (0,0);
                        let merge_stats = |t1: (usize, usize), t2: (usize,usize)| (t1.0 + t2.0, t1.1 + t2.1);
                        let transmit_stats = if shreds.len() < PAR_ITER_MIN_NUM_SHREDS {
                            shreds
                                .into_iter()
                                .enumerate()
                                .map(|(index, ((key, shred), cluster_nodes))| {
                                    let (_root_distance, transmit_attempts, transmit_errors) = retransmit_shred(
                                        &key,
                                        &shred,
                                        &cluster_nodes,
                                        &socket_addr_space,
                                        &clique_outbound_sockets[index % clique_outbound_sockets.len()],
                                    );
                                    (transmit_attempts, transmit_errors)
                                })
                                .fold(init_stats,merge_stats)
                        } else {
                            thread_pool.install(|| {
                                shreds
                                    .into_par_iter()
                                    .map(|((key, shred), cluster_nodes)| {
                                        let index = thread_pool.current_thread_index().unwrap();
                                        let (_root_distance, transmit_attempts, transmit_errors) = retransmit_shred(
                                            &key,
                                            &shred,
                                            &cluster_nodes,
                                            &socket_addr_space,
                                            &clique_outbound_sockets[index % clique_outbound_sockets.len()],
                                        );
                                        (transmit_attempts, transmit_errors)
                                        
                                    })
                                    .fold(|| init_stats, merge_stats)
                                    .reduce(|| init_stats, merge_stats)
                            })
                        };
                        transmit_execute.stop();
                        stats.transmit_execute_ns += transmit_execute.as_ns();
                        stats.transmit_attempts += transmit_stats.0;
                        stats.transmit_errors += transmit_stats.1;
                    } // END - recv
                    stats.maybe_submit();
                } // END - while
            }) // END - spawn
            .unwrap();

        Self { gossip_thread_hdl, outbound_thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.outbound_thread_hdl.join()?;
        self.gossip_thread_hdl.join()?;
        Ok(())
    }
}

// copied and adapted from retransmit_stage.rs
fn retransmit_shred(
    key: &ShredId,
    shred: &[u8],
    cluster_nodes: &ClusterNodes<CliqueStage>,
    socket_addr_space: &SocketAddrSpace,
    socket: &UdpSocket,
) -> (/*root_distance:*/ usize, /*num_nodes:*/ usize, /*num_failed*/ usize) {
    let (root_distance, addrs) =
        cluster_nodes.get_retransmit_addrs( key, DATA_PLANE_FANOUT);

    let addrs: Vec<_> = addrs
        .into_iter()
        .filter(|addr| LegacyContactInfo::is_valid_address(addr, socket_addr_space))
        .collect();
    
    let num_failed = match multi_target_send(socket, shred, &addrs) {
        Ok(()) => 0,
        Err(SendPktsError::IoError(ioerr, num_failed)) => {
            error!(
                "retransmit_to multi_target_send error: {:?}, {}/{} packets failed",
                ioerr,
                num_failed,
                addrs.len(),
            );
            num_failed
        }
    };

    (root_distance, addrs.len(), num_failed)
}

// libp2p adds a few extra bytes in the beginning to tag peer ids
fn peer_id_to_solana_pubkey(
    peer_id: PeerId,
) -> Pubkey {
    let bytes = peer_id.to_bytes();
    let split_index = bytes.len() - 32;
    let pk_bytes = <&[u8; 32]>::try_from(&bytes[split_index..]).unwrap();
    Pubkey::new_from_array(*pk_bytes)
}

fn socket_addr_to_multi_addr(
    socket: SocketAddr
) -> Multiaddr {
    let mut addr = Multiaddr::from(socket.ip());
    addr.push(Protocol::Tcp(socket.port()));
    return addr
}