// this is a service to handle bidirections quinn channel
use {
    bincode::serialize,
    quinn::SendStream,
    serde::{Deserialize, Serialize},
    solana_perf::packet::{Packet, PacketBatch},
    solana_sdk::{pubkey::Pubkey, signature::Signature, slot_history::Slot},
    std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::{
        runtime::Builder,
        sync::{
            mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
            RwLock,
        },
    },
};

// This message size if fixed so that we know we that we have recieved a new message after getting QUIC_REPLY_MESSAGE_SIZE bytes
// when you add any new message type please make sure that it is exactly of size QUIC_REPLY_MESSAGE_SIZE
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum QuicReplyMessage {
    TransactionExecutionMessage {
        leader_identity: Pubkey,
        transaction_signature: Signature,
        message: Vec<u8>,
        approximate_slot: Slot,
        unused: [u8;12],
    },
}
pub const QUIC_REPLY_MESSAGE_STRING_SIZE: usize = 128;
pub const QUIC_REPLY_MESSAGE_SIZE: usize = 256;

impl QuicReplyMessage {
    pub fn new_transaction_execution_message(
        identity: Pubkey,
        transaction_signature: Signature,
        message: String,
        slot: Slot,
    ) -> Self {
        let mut message = message.as_bytes().to_vec();
        message.resize(QUIC_REPLY_MESSAGE_STRING_SIZE, 0);
        Self::TransactionExecutionMessage {
            leader_identity: identity,
            message,
            transaction_signature: transaction_signature,
            approximate_slot: slot,
            unused: [0;12],
        }
    }
}

const TRANSACTION_TIMEOUT: u64 = 30_000; // 30s

pub struct QuicBidirectionalData {
    pub transaction_signature_map: HashMap<Signature, Vec<u64>>,
    pub sender_ids_map: HashMap<u64, UnboundedSender<QuicReplyMessage>>,
    pub sender_socket_address_map: HashMap<SocketAddr, u64>,
    pub last_id: u64,
}

#[derive(Clone)]
pub struct QuicBidirectionalMetrics {
    pub connections_added: Arc<AtomicU64>,
    pub transactions_added: Arc<AtomicU64>,
    pub transactions_replied_to: Arc<AtomicU64>,
    pub transactions_removed: Arc<AtomicU64>,
    pub connections_disconnected: Arc<AtomicU64>,
    pub connections_errors: Arc<AtomicU64>,
}

impl QuicBidirectionalMetrics {
    pub fn new() -> Self {
        Self {
            connections_added: Arc::new(AtomicU64::new(0)),
            transactions_added: Arc::new(AtomicU64::new(0)),
            transactions_replied_to: Arc::new(AtomicU64::new(0)),
            connections_disconnected: Arc::new(AtomicU64::new(0)),
            transactions_removed: Arc::new(AtomicU64::new(0)),
            connections_errors: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn connections_added(&self) -> u64 {
        self.connections_added.load(Ordering::Relaxed)
    }

    pub fn transactions_added(&self) -> u64 {
        self.transactions_added.load(Ordering::Relaxed)
    }

    pub fn transactions_replied_to(&self) -> u64 {
        self.transactions_replied_to.load(Ordering::Relaxed)
    }

    pub fn transactions_removed(&self) -> u64 {
        self.transactions_removed.load(Ordering::Relaxed)
    }

    pub fn connections_disconnected(&self) -> u64 {
        self.connections_disconnected.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct QuicBidirectionalReplyService {
    data: Option<Arc<RwLock<QuicBidirectionalData>>>,
    service_sender: Option<UnboundedSender<QuicReplyMessage>>,
    serving_handle: Option<Arc<std::thread::JoinHandle<()>>>,
    do_exit: Arc<AtomicBool>,
    pub metrics: QuicBidirectionalMetrics,
    pub identity: Pubkey,
    // usually this is updated during banking stage, so it is the slot when validator was a leader
    pub last_known_slot: Arc<AtomicU64>,
}

pub fn get_signature_from_packet(packet: &Packet) -> Option<Signature> {
    // add instruction signature for message
    match packet.data(1..65) {
        Some(signature_bytes) => {
            let sig = Signature::new(&signature_bytes);
            Some(sig)
        }
        None => None,
    }
}

impl QuicBidirectionalReplyService {
    pub fn new(identity: Pubkey) -> Self {
        let (service_sender, service_reciever) = unbounded_channel();
        let mut instance = Self {
            service_sender: Some(service_sender),
            data: Some(Arc::new(RwLock::new(QuicBidirectionalData {
                transaction_signature_map: HashMap::new(),
                sender_ids_map: HashMap::new(),
                sender_socket_address_map: HashMap::new(),
                last_id: 1,
            }))),
            serving_handle: None,
            metrics: QuicBidirectionalMetrics::new(),
            identity: identity,
            do_exit: Arc::new(AtomicBool::new(false)),
            last_known_slot: Arc::new(AtomicU64::new(0)),
        };
        instance.serve(service_reciever);
        instance
    }

    pub fn disabled() -> Self {
        Self {
            data: None,
            service_sender: None,
            serving_handle: None,
            do_exit: Arc::new(AtomicBool::new(true)),
            metrics: QuicBidirectionalMetrics::new(),
            identity: Pubkey::default(),
            last_known_slot: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn send_message(&self, transaction_signature: &Signature, message: String) {
        if let Some(sender) = &self.service_sender {
            let current_slot = self.last_known_slot.load(Ordering::Relaxed);
            let message = QuicReplyMessage::new_transaction_execution_message(
                self.identity,
                *transaction_signature,
                message,
                current_slot,
            );
            match sender.send(message) {
                Err(e) => {
                    debug!("send error {}", e);
                }
                _ => {
                    // continue
                }
            }
        }
    }

    pub fn update_slot(&self, slot: u64) {
        self.last_known_slot.store(slot, Ordering::Relaxed);
    }

    // When we get a bidirectional stream then we add the send stream to the service.
    // This will create a crossbeam channel to dispatch messages and a task which will listen to the crossbeam channel and will send the replies back throught send stream.
    // When you add again a new send_stream channel for the same socket address then we will remove the previous one from the socket address map,
    // This will then destroy the sender of the crossbeam channel putting the reciever_channel in error state starting the clean up sequence for the old channel.
    // So when you add again a new channel for same socket, we will no longer get the messages for old channel.
    pub async fn add_stream(&self, quic_address: SocketAddr, send_stream: SendStream) {
        if self.data.is_none() {
            return;
        }
        let data = self.data.as_ref().unwrap().clone();
        let (sender_channel, reciever_channel) = unbounded_channel();
        // context for writelocking the data
        let sender_id = {
            let mut data = data.write().await;
            let data = &mut *data;
            if let Some(x) = data.sender_socket_address_map.get(&quic_address) {
                // remove existing channel and replace with new one
                // removing this channel should also destroy the p
                data.sender_ids_map.remove(x);
            };
            let current_id = data.last_id;
            data.last_id += 1;
            data.sender_ids_map
                .insert(current_id, sender_channel.clone());
            data.sender_socket_address_map
                .insert(quic_address.clone(), current_id);
            // create a new or replace the exisiting id
            data.sender_ids_map
                .insert(current_id, sender_channel.clone());
            data.sender_socket_address_map
                .insert(quic_address.clone(), current_id);
            current_id
        };

        self.metrics
            .connections_added
            .fetch_add(1, Ordering::Relaxed);
        let subscriptions = data.clone();
        let metrics = self.metrics.clone();
        let do_exit = self.do_exit.clone();

        // start listnening to stream specific cross beam channel
        tokio::spawn(async move {
            let mut send_stream = send_stream;
            let mut reciever_channel = reciever_channel;
            loop {
                let finish = tokio::select! {
                    message = reciever_channel.recv() => {
                        if let Some(message) = message {
                            let serialized_message = serialize(&message).unwrap();

                            let res = send_stream.write_all(serialized_message.as_slice()).await;
                            if let Err(error) = res {
                                info!(
                                    "Bidirectional writing stopped for socket {} because {}",
                                    quic_address,
                                    error.to_string()
                                );
                                metrics.connections_errors.fetch_add(1, Ordering::Relaxed);
                                true
                            } else {
                                metrics
                                .transactions_replied_to
                                .fetch_add(1, Ordering::Relaxed);
                                false
                            }
                        } else {
                            trace!("recv channel closed");
                            true
                        }
                    },
                    task = tokio::time::timeout( Duration::from_millis(100),send_stream.stopped()) => {
                        if  task.is_err() {
                            // timeout
                            do_exit.load(Ordering::Relaxed)
                        } else {
                            // client stopped the stream
                            true
                        }
                    }
                };

                if finish {
                    trace!("finishing the stream");
                    let _ = send_stream.finish().await;
                    break;
                }
            }
            // remove all data belonging to sender_id
            let mut sub_data = subscriptions.write().await;
            let subscriptions = &mut *sub_data;
            subscriptions.sender_ids_map.remove(&sender_id);
            subscriptions
                .sender_socket_address_map
                .retain(|_, v| *v != sender_id);

            metrics
                .connections_disconnected
                .fetch_add(1, Ordering::Relaxed);
        });
    }

    pub async fn add_packets(&self, quic_address: &SocketAddr, packets: &PacketBatch) {
        if self.data.is_none() {
            return;
        }
        let data = self.data.as_ref().unwrap().clone();

        // check if socket is registered;
        let id = {
            let data = data.read().await;
            data.sender_socket_address_map
                .get(quic_address)
                .map_or(0, |x| *x)
        };

        // this means that there is not bidirectional connection, and packets came from unidirectional socket
        if id == 0 {
            return;
        }
        let mut data_wl = data.write().await;
        let data_mut = &mut *data_wl;
        let metrics = self.metrics.clone();
        packets.iter().for_each(|packet| {
            let meta = &packet.meta();
            if meta.discard()
                || meta.forwarded()
                || meta.is_simple_vote_tx()
                || meta.is_tracer_packet()
                || meta.repair()
            {
                return;
            }
            let signature = get_signature_from_packet(packet);
            metrics.transactions_added.fetch_add(1, Ordering::Relaxed);
            signature.map(|x| {
                let ids = data_mut.transaction_signature_map.get_mut(&x);
                match ids {
                    Some(ids) => ids.push(id), // push in exisiting ids
                    None => {
                        data_mut.transaction_signature_map.insert(x, vec![id]); // push a new id vector

                        // create a task to clean up Timedout transactions
                        let data = data.clone();
                        let metrics = metrics.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(TRANSACTION_TIMEOUT)).await;
                            let mut data = data.write().await;
                            data.transaction_signature_map.remove(&x);
                            metrics.transactions_removed.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                }
            });
        });
    }

    // this method will start bidirectional relay service
    // the the message sent to bidirectional service,
    // will be dispactched to the appropriate sender channel
    // depending on transcation signature or message hash
    pub fn serve(&mut self, service_reciever: UnboundedReceiver<QuicReplyMessage>) {
        let subscription_data = self.data.as_ref().unwrap().clone();

        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let do_exit = self.do_exit.clone();
        let handle = std::thread::spawn(move || {
            let mut service_reciever = service_reciever;
            let runtime = runtime;

            let subscription_data = subscription_data.clone();
            loop {
                let do_exit = do_exit.clone();
                let bidirectional_message = runtime.block_on(async {
                    let do_exit = do_exit.clone();
                    loop {
                        if let Ok(recved) = tokio::time::timeout(
                            Duration::from_millis(100),
                            service_reciever.recv(),
                        )
                        .await
                        {
                            return recved;
                        } else {
                            let should_exit = do_exit.load(Ordering::Relaxed);
                            if should_exit {
                                return None;
                            }
                        }
                    }
                });
                if bidirectional_message.is_none() {
                    // the channel has be closed
                    trace!("quic bidirectional channel is closed");
                    break;
                }
                let message = bidirectional_message.unwrap();

                let data = runtime.block_on(subscription_data.read());
                match message {
                    QuicReplyMessage::TransactionExecutionMessage {
                        transaction_signature,
                        ..
                    } => {
                        // if the message has transaction signature then find stream from transaction signature
                        // else find stream by packet hash
                        let send_stream_ids = data
                            .transaction_signature_map
                            .get(&transaction_signature)
                            .map(|x| x);
                        if let Some(send_stream_ids) = send_stream_ids {
                            for send_stream_id in send_stream_ids {
                                if let Some(send_stream) = data.sender_ids_map.get(&send_stream_id)
                                {
                                    match send_stream.send(message.clone()) {
                                        Err(e) => {
                                            warn!(
                                                "Error sending a bidirectional message {}",
                                                e.to_string()
                                            );
                                        }
                                        Ok(_) => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        self.serving_handle = Some(Arc::new(handle));
    }
}