//! # The Asynchronous EventLoop Engine
//!
//! Owns the active network transport, executes keep-alive ping schedules,
//! dispatches incoming publishes to topic subscription streams, manages
//! auto-reconnect backoff, offline queues, and session data recovery.

use std::collections::{HashMap, VecDeque};
use std::string::{String, ToString};
use std::time::Duration;
use std::vec::Vec;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, Instant};

use crate::options::{ClientOptions, DropStrategy};
use crate::router::TopicRouter;
use crate::transport::{BoxedTransport, connect_transport};
use crate::types::{ClientError, ClientRequest, ConnectionStatus, PublishMessage};
use mqtt_embedded::ProtocolError;
use mqtt_packet::{
    Connect, Disconnect, EncodePacket, MqttPacket, PingReq, PubAck, Publish, QoS,
    RawPacketFrameIter, Subscribe, Unsubscribe, Will, decode,
};

enum IncomingAction {
    Publish(PublishMessage),
    PubAck(u16),
    SubAck(u16),
    UnsubAck(u16),
    PingResp,
    Disconnect,
}

/// The internal asynchronous event loop managing the socket, MQTT state machine, and session data recovery.
#[allow(clippy::type_complexity)]
pub struct EventLoop {
    options: ClientOptions,
    router: TopicRouter,
    req_rx: mpsc::Receiver<ClientRequest>,
    status_tx: watch::Sender<ConnectionStatus>,
    transport: Option<BoxedTransport>,
    offline_queue: VecDeque<PublishMessage>,
    active_subscriptions: HashMap<String, QoS>,
    inflight_publishes: HashMap<
        u16,
        (
            PublishMessage,
            Option<oneshot::Sender<Result<(), ClientError>>>,
        ),
    >,
    inflight_subscribes: HashMap<u16, oneshot::Sender<Result<u16, ClientError>>>,
    inflight_unsubscribes: HashMap<u16, oneshot::Sender<Result<u16, ClientError>>>,
    next_packet_id: u16,
    rx_buf: Vec<u8>,
    tx_buf: Vec<u8>,
    last_tx: Instant,
    reconnect_attempt: usize,
}

impl EventLoop {
    pub(crate) fn new(
        options: ClientOptions,
        req_rx: mpsc::Receiver<ClientRequest>,
        status_tx: watch::Sender<ConnectionStatus>,
    ) -> Self {
        let max_pkt = options.max_packet_size;
        Self {
            options,
            router: TopicRouter::new(),
            req_rx,
            status_tx,
            transport: None,
            offline_queue: VecDeque::new(),
            active_subscriptions: HashMap::new(),
            inflight_publishes: HashMap::new(),
            inflight_subscribes: HashMap::new(),
            inflight_unsubscribes: HashMap::new(),
            next_packet_id: 1,
            rx_buf: std::vec::from_elem(0u8, max_pkt),
            tx_buf: std::vec::from_elem(0u8, max_pkt),
            last_tx: Instant::now(),
            reconnect_attempt: 0,
        }
    }

    /// Allocates the next rolling MQTT packet ID (1..=65535).
    fn get_next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }

    /// Runs the infinite event loop driver until explicitly stopped or when all client handles are dropped.
    pub async fn run(&mut self) {
        loop {
            // If not connected, attempt connection
            if self.transport.is_none() && self.connect_with_backoff().await.is_err() {
                let _ = self.status_tx.send(ConnectionStatus::Stopped);
                tracing::error!("MQTT connection failed permanently.");
                break;
            }

            // Drive connection active cycle
            if let Err(err) = self.drive_connection().await {
                if matches!(err, ClientError::ClientClosed) {
                    let _ = self.status_tx.send(ConnectionStatus::Stopped);
                    break;
                }
                tracing::warn!(
                    "MQTT connection dropped: {err}. Initiating reconnection & data recovery..."
                );
                self.transport = None;
                let _ = self.status_tx.send(ConnectionStatus::Disconnected);
                if !self.options.reconnect.enabled {
                    let _ = self.status_tx.send(ConnectionStatus::Stopped);
                    break;
                }
            }
        }
    }

    /// Attempts to connect to the broker with exponential backoff and jitter.
    async fn connect_with_backoff(&mut self) -> Result<(), ClientError> {
        loop {
            self.reconnect_attempt += 1;
            let delay = self.options.reconnect.compute_delay(self.reconnect_attempt);

            if delay > Duration::ZERO {
                let _ = self.status_tx.send(ConnectionStatus::Reconnecting {
                    attempt: self.reconnect_attempt,
                    next_retry: delay,
                });
                time::sleep(delay).await;
            }

            let _ = self.status_tx.send(ConnectionStatus::Connecting);
            tracing::info!(
                "Connecting to broker (attempt {})...",
                self.reconnect_attempt
            );

            match connect_transport(&self.options.target).await {
                Ok(mut transport) => {
                    // Perform MQTT Connect Handshake
                    match self.perform_connect_handshake(&mut transport).await {
                        Ok(()) => {
                            tracing::info!("MQTT Connected successfully!");
                            self.transport = Some(transport);
                            self.reconnect_attempt = 0;
                            let _ = self.status_tx.send(ConnectionStatus::Connected);
                            self.last_tx = Instant::now();

                            // Perform session data recovery & subscription restoration
                            self.perform_session_data_recovery().await?;
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::warn!("MQTT Handshake error: {e}");
                            if let Some(max) = self.options.reconnect.max_retries
                                && self.reconnect_attempt >= max
                            {
                                return Err(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Transport connection error: {e}");
                    if let Some(max) = self.options.reconnect.max_retries
                        && self.reconnect_attempt >= max
                    {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Performs the initial MQTT `CONNECT` packet transmission and awaits `CONNACK`.
    async fn perform_connect_handshake(
        &mut self,
        transport: &mut BoxedTransport,
    ) -> Result<(), ClientError> {
        let keep_alive_secs = self.options.keep_alive.as_secs().min(65535) as u16;
        let mut connect = Connect::new(
            &self.options.client_id,
            keep_alive_secs,
            self.options.clean_session,
        );
        connect.username = self.options.username.as_deref();
        connect.password = self.options.password.as_deref();

        let will_data;
        if let Some(ref will) = self.options.will {
            will_data = Will::new(&will.topic, &will.payload, will.qos, will.retain);
            connect.will = Some(will_data);
        }

        let len = connect
            .encode(&mut self.tx_buf, self.options.version)
            .map_err(|_| ClientError::Protocol(ProtocolError::MalformedPacket))?;

        transport.write_all(&self.tx_buf[..len]).await?;
        transport.flush().await?;

        // Wait for CONNACK with timeout
        let read_len = time::timeout(Duration::from_secs(10), transport.read(&mut self.rx_buf))
            .await
            .map_err(|_| ClientError::Timeout)??;

        if read_len == 0 {
            return Err(ClientError::Protocol(ProtocolError::InvalidResponse));
        }

        let packet = decode(&self.rx_buf[..read_len], self.options.version)
            .map_err(|_| ClientError::Protocol(ProtocolError::InvalidResponse))?
            .ok_or(ClientError::Protocol(ProtocolError::InvalidResponse))?;

        if let MqttPacket::ConnAck(connack) = packet {
            if connack.reason_code == 0 {
                Ok(())
            } else {
                Err(ClientError::ConnectionRefused(connack.reason_code))
            }
        } else {
            Err(ClientError::Protocol(ProtocolError::InvalidResponse))
        }
    }

    /// Performs automatic session data recovery:
    /// 1. Resends unacknowledged in-flight QoS 1/2 messages with `DUP=true`.
    /// 2. Restores all active topic subscriptions.
    /// 3. Flushes queued offline publishes.
    async fn perform_session_data_recovery(&mut self) -> Result<(), ClientError> {
        let mut transport = self.transport.take().ok_or(ClientError::NotConnected)?;

        // 1. Recover in-flight unacknowledged messages
        if self.options.recovery.resend_unacked_inflight && !self.inflight_publishes.is_empty() {
            tracing::info!(
                "Recovering {} unacknowledged in-flight QoS messages...",
                self.inflight_publishes.len()
            );
            let in_flight: Vec<(u16, PublishMessage)> = self
                .inflight_publishes
                .iter()
                .map(|(pid, (msg, _))| (*pid, msg.clone()))
                .collect();

            for (pid, mut msg) in in_flight {
                msg.dup = true;
                msg.packet_id = Some(pid);

                let mut pub_packet = Publish::new(&msg.topic, &msg.payload, msg.qos);
                pub_packet.packet_id = Some(pid);
                pub_packet.retain = msg.retain;
                pub_packet.dup = true;

                if let Ok(len) = pub_packet.encode(&mut self.tx_buf, self.options.version) {
                    let _ = transport.write_all(&self.tx_buf[..len]).await;
                }
            }
            let _ = transport.flush().await;
        }

        // 2. Restore active subscriptions
        if self.options.recovery.auto_resubscribe && !self.active_subscriptions.is_empty() {
            tracing::info!(
                "Restoring {} active topic subscriptions...",
                self.active_subscriptions.len()
            );
            let subs: Vec<(String, QoS)> = self
                .active_subscriptions
                .iter()
                .map(|(t, q)| (t.clone(), *q))
                .collect();

            for (topic, qos) in subs {
                let pid = self.get_next_packet_id();
                let mut sub = Subscribe::new(pid);
                let _ = sub.add_topic(&topic, qos);
                if let Ok(len) = sub.encode(&mut self.tx_buf, self.options.version) {
                    let _ = transport.write_all(&self.tx_buf[..len]).await;
                }
            }
            let _ = transport.flush().await;
        }

        // 3. Flush offline queue
        while let Some(msg) = self.offline_queue.pop_front() {
            let _ = self.send_publish_direct(&mut transport, msg, None).await;
        }

        self.transport = Some(transport);
        Ok(())
    }

    /// The active connection processing loop.
    async fn drive_connection(&mut self) -> Result<(), ClientError> {
        let ping_interval = self.options.keep_alive;
        let mut ping_timer = time::interval(ping_interval);
        ping_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let mut transport = self.transport.take().ok_or(ClientError::NotConnected)?;
        let mut rx_cursor = 0;

        let res: Result<(), ClientError> = async {
            loop {
                tokio::select! {
                    // 1. Read incoming bytes from the socket
                    read_res = transport.read(&mut self.rx_buf[rx_cursor..]) => {
                        let n = read_res?;
                        if n == 0 {
                            return Err(ClientError::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Broker closed connection",
                            )));
                        }
                        rx_cursor += n;

                        // Parse all full frames received
                        let mut actions = Vec::new();
                        let mut consumed = 0;
                        {
                            let iter = RawPacketFrameIter::new(&self.rx_buf[..rx_cursor]);
                            for frame in iter.flatten() {
                                consumed += frame.len();
                                if let Some(packet) = decode(frame, self.options.version)
                                    .map_err(|_| ClientError::Protocol(ProtocolError::InvalidResponse))?
                                {
                                    match packet {
                                        MqttPacket::Publish(p) => {
                                            let owned_msg = PublishMessage {
                                                topic: p.topic.to_string(),
                                                payload: Bytes::copy_from_slice(p.payload),
                                                qos: p.qos,
                                                retain: p.retain,
                                                dup: p.dup,
                                                packet_id: p.packet_id,
                                                user_properties: Vec::new(),
                                            };
                                            actions.push(IncomingAction::Publish(owned_msg));
                                        }
                                        MqttPacket::PubAck(ack) => {
                                            actions.push(IncomingAction::PubAck(ack.packet_id));
                                        }
                                        MqttPacket::SubAck(suback) => {
                                            actions.push(IncomingAction::SubAck(suback.packet_id));
                                        }
                                        MqttPacket::UnsubAck(unsuback) => {
                                            actions.push(IncomingAction::UnsubAck(unsuback.packet_id));
                                        }
                                        MqttPacket::PingResp => {
                                            actions.push(IncomingAction::PingResp);
                                        }
                                        MqttPacket::Disconnect(_) => {
                                            actions.push(IncomingAction::Disconnect);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }

                        if consumed > 0 {
                            self.rx_buf.copy_within(consumed..rx_cursor, 0);
                            rx_cursor -= consumed;
                        }

                        for action in actions {
                            match action {
                                IncomingAction::Publish(owned_msg) => {
                                    if owned_msg.qos == QoS::AtLeastOnce && let Some(pid) = owned_msg.packet_id {
                                        let ack = PubAck::new(pid);
                                        if let Ok(len) = ack.encode(&mut self.tx_buf, self.options.version) {
                                            transport.write_all(&self.tx_buf[..len]).await?;
                                            transport.flush().await?;
                                            self.last_tx = Instant::now();
                                        }
                                    }
                                    self.router.dispatch(&owned_msg);
                                }
                                IncomingAction::PubAck(packet_id) => {
                                    if let Some((_, Some(sender))) = self.inflight_publishes.remove(&packet_id) {
                                        let _ = sender.send(Ok(()));
                                    }
                                }
                                IncomingAction::SubAck(packet_id) => {
                                    if let Some(sender) = self.inflight_subscribes.remove(&packet_id) {
                                        let _ = sender.send(Ok(packet_id));
                                    }
                                }
                                IncomingAction::UnsubAck(packet_id) => {
                                    if let Some(sender) = self.inflight_unsubscribes.remove(&packet_id) {
                                        let _ = sender.send(Ok(packet_id));
                                    }
                                }
                                IncomingAction::PingResp => {
                                    tracing::trace!("MQTT Ping response received");
                                }
                                IncomingAction::Disconnect => {
                                    tracing::info!("MQTT broker initiated disconnect");
                                    return Err(ClientError::NotConnected);
                                }
                            }
                        }
                    }

                    // 2. Handle outgoing client requests from channels
                    req_opt = self.req_rx.recv() => {
                        match req_opt {
                            Some(req) => self.handle_client_request(&mut transport, req).await?,
                            None => {
                                // All client handles dropped -> graceful disconnect
                                let _ = self.send_disconnect(&mut transport).await;
                                return Ok(());
                            }
                        }
                    }

                    // 3. Keep-alive Ping timer tick
                    _ = ping_timer.tick() => {
                        if self.last_tx.elapsed() >= ping_interval {
                            self.send_ping(&mut transport).await?;
                        }
                    }
                }
            }
        }.await;

        self.transport = Some(transport);
        res
    }

    /// Handles a request from the `AsyncClient` handle.
    async fn handle_client_request(
        &mut self,
        transport: &mut BoxedTransport,
        request: ClientRequest,
    ) -> Result<(), ClientError> {
        match request {
            ClientRequest::Publish {
                message,
                ack_sender,
            } => {
                self.send_publish_direct(transport, message, ack_sender)
                    .await?;
            }
            ClientRequest::PublishBatch {
                messages,
                ack_sender,
            } => {
                let count = self.send_publish_batch_direct(transport, messages).await?;
                if let Some(sender) = ack_sender {
                    let _ = sender.send(Ok(count));
                }
            }
            ClientRequest::PublishDatagram {
                topic,
                payload,
                ack_sender,
            } => {
                let msg = PublishMessage::new(&topic, payload);
                let pub_packet = Publish::new(&topic, &msg.payload, QoS::AtMostOnce);
                let len = pub_packet
                    .encode(&mut self.tx_buf, self.options.version)
                    .map_err(|_| ClientError::Protocol(ProtocolError::MalformedPacket))?;

                let res = transport.send_datagram(&self.tx_buf[..len]);
                if let Some(sender) = ack_sender {
                    let _ = sender.send(res);
                }
            }
            ClientRequest::Subscribe {
                topic,
                qos,
                resp_sender,
                stream_sender,
            } => {
                let pid = self.get_next_packet_id();
                if let Some(stream_tx) = stream_sender {
                    let _ = self.router.insert(&topic, stream_tx);
                }
                self.active_subscriptions.insert(topic.clone(), qos);

                let mut sub = Subscribe::new(pid);
                let _ = sub.add_topic(&topic, qos);
                let len = sub
                    .encode(&mut self.tx_buf, self.options.version)
                    .map_err(|_| ClientError::Protocol(ProtocolError::MalformedPacket))?;

                transport.write_all(&self.tx_buf[..len]).await?;
                transport.flush().await?;
                self.last_tx = Instant::now();

                self.inflight_subscribes.insert(pid, resp_sender);
            }
            ClientRequest::Unsubscribe { topic, resp_sender } => {
                let pid = self.get_next_packet_id();
                let _ = self.router.remove(&topic);
                self.active_subscriptions.remove(&topic);

                let mut unsub = Unsubscribe::new(pid);
                let _ = unsub.add_topic(&topic);
                let len = unsub
                    .encode(&mut self.tx_buf, self.options.version)
                    .map_err(|_| ClientError::Protocol(ProtocolError::MalformedPacket))?;

                transport.write_all(&self.tx_buf[..len]).await?;
                transport.flush().await?;
                self.last_tx = Instant::now();

                self.inflight_unsubscribes.insert(pid, resp_sender);
            }
            ClientRequest::Disconnect { resp_sender } => {
                let res = self.send_disconnect(transport).await;
                let _ = resp_sender.send(res);
                return Err(ClientError::ClientClosed);
            }
        }
        Ok(())
    }

    /// Sends a publish direct or buffers offline if not currently connected.
    #[allow(dead_code)]
    pub(crate) async fn send_publish(
        &mut self,
        message: PublishMessage,
        ack_sender: Option<oneshot::Sender<Result<(), ClientError>>>,
    ) -> Result<(), ClientError> {
        if let Some(mut transport) = self.transport.take() {
            let res = self
                .send_publish_direct(&mut transport, message, ack_sender)
                .await;
            self.transport = Some(transport);
            res
        } else {
            // Buffer offline
            if self.options.offline_queue.capacity == 0 {
                return Err(ClientError::NotConnected);
            }
            if self.offline_queue.len() >= self.options.offline_queue.capacity {
                match self.options.offline_queue.drop_strategy {
                    DropStrategy::DropOldest => {
                        self.offline_queue.pop_front();
                    }
                    DropStrategy::ErrorOnFull => {
                        return Err(ClientError::QueueFull);
                    }
                    DropStrategy::Block => {
                        return Err(ClientError::QueueFull);
                    }
                }
            }
            self.offline_queue.push_back(message);
            if let Some(ack) = ack_sender {
                let _ = ack.send(Ok(()));
            }
            Ok(())
        }
    }

    async fn send_publish_direct(
        &mut self,
        transport: &mut BoxedTransport,
        mut message: PublishMessage,
        ack_sender: Option<oneshot::Sender<Result<(), ClientError>>>,
    ) -> Result<(), ClientError> {
        let packet_id = if message.qos != QoS::AtMostOnce {
            let pid = self.get_next_packet_id();
            message.packet_id = Some(pid);
            self.inflight_publishes
                .insert(pid, (message.clone(), ack_sender));
            Some(pid)
        } else {
            if let Some(ack) = ack_sender {
                let _ = ack.send(Ok(()));
            }
            None
        };

        let mut pub_packet = Publish::new(&message.topic, &message.payload, message.qos);
        pub_packet.packet_id = packet_id;
        pub_packet.retain = message.retain;
        pub_packet.dup = message.dup;

        let len = pub_packet
            .encode(&mut self.tx_buf, self.options.version)
            .map_err(|_| ClientError::Protocol(ProtocolError::MalformedPacket))?;

        transport.write_all(&self.tx_buf[..len]).await?;
        transport.flush().await?;
        self.last_tx = Instant::now();
        Ok(())
    }

    /// Fast-path multi-packet burst sending: encodes multiple publishes and writes to transport.
    async fn send_publish_batch_direct(
        &mut self,
        transport: &mut BoxedTransport,
        messages: Vec<PublishMessage>,
    ) -> Result<usize, ClientError> {
        let mut cursor = 0;
        let mut sent_count = 0;

        for mut msg in messages {
            let packet_id = if msg.qos != QoS::AtMostOnce {
                let pid = self.get_next_packet_id();
                msg.packet_id = Some(pid);
                self.inflight_publishes.insert(pid, (msg.clone(), None));
                Some(pid)
            } else {
                None
            };

            let mut pub_packet = Publish::new(&msg.topic, &msg.payload, msg.qos);
            pub_packet.packet_id = packet_id;
            pub_packet.retain = msg.retain;
            pub_packet.dup = msg.dup;

            match pub_packet.encode(&mut self.tx_buf[cursor..], self.options.version) {
                Ok(len) => {
                    cursor += len;
                    sent_count += 1;
                }
                Err(_) => {
                    // Flush current buffer and retry this packet
                    if cursor > 0 {
                        transport.write_all(&self.tx_buf[..cursor]).await?;
                        cursor = 0;
                        let len = pub_packet
                            .encode(&mut self.tx_buf, self.options.version)
                            .map_err(|_| ClientError::Protocol(ProtocolError::MalformedPacket))?;
                        cursor += len;
                        sent_count += 1;
                    }
                }
            }
        }

        if cursor > 0 {
            transport.write_all(&self.tx_buf[..cursor]).await?;
            transport.flush().await?;
            self.last_tx = Instant::now();
        }

        Ok(sent_count)
    }

    async fn send_ping(&mut self, transport: &mut BoxedTransport) -> Result<(), ClientError> {
        let len = PingReq
            .encode(&mut self.tx_buf, self.options.version)
            .map_err(|_| ClientError::Protocol(ProtocolError::MalformedPacket))?;
        transport.write_all(&self.tx_buf[..len]).await?;
        transport.flush().await?;
        self.last_tx = Instant::now();
        Ok(())
    }

    async fn send_disconnect(&mut self, transport: &mut BoxedTransport) -> Result<(), ClientError> {
        let disc = Disconnect::new();
        if let Ok(len) = disc.encode(&mut self.tx_buf, self.options.version) {
            let _ = transport.write_all(&self.tx_buf[..len]).await;
            let _ = transport.flush().await;
        }
        let _ = self.status_tx.send(ConnectionStatus::Stopped);
        Ok(())
    }
}
