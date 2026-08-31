//! # The Asynchronous Client Handle
//!
//! Provides a lightweight, cloneable [`AsyncClient`] handle for publishing,
//! subscribing, creating topic-filtered [`TopicSubscription`] streams,
//! and sending real-time QUIC datagrams.

use std::string::String;
use std::vec::Vec;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::packet::QoS;
use crate::tokio_client::eventloop::EventLoop;
use crate::tokio_client::options::ClientOptions;
use crate::tokio_client::types::{
    ClientError, ClientRequest, ConnectionStatus, PublishMessage, TopicSubscription,
};

/// A lightweight, cheaply cloneable handle to communicate with the MQTT background driver.
#[derive(Clone)]
pub struct AsyncClient {
    req_tx: mpsc::Sender<ClientRequest>,
    status_rx: watch::Receiver<ConnectionStatus>,
}

impl AsyncClient {
    pub(crate) fn new(
        req_tx: mpsc::Sender<ClientRequest>,
        status_rx: watch::Receiver<ConnectionStatus>,
    ) -> Self {
        Self { req_tx, status_rx }
    }

    /// Publishes a message asynchronously without blocking for acknowledgment (fire and forget for QoS 0).
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        qos: QoS,
        retain: bool,
        payload: impl Into<Bytes>,
    ) -> Result<(), ClientError> {
        let msg = PublishMessage {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            retain,
            dup: false,
            packet_id: None,
            user_properties: Vec::new(),
        };

        self.req_tx
            .send(ClientRequest::Publish {
                message: msg,
                ack_sender: None,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        Ok(())
    }

    /// Publishes a message and awaits acknowledgment (PUBACK for QoS 1).
    pub async fn publish_with_ack(
        &self,
        topic: impl Into<String>,
        qos: QoS,
        retain: bool,
        payload: impl Into<Bytes>,
    ) -> Result<(), ClientError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let msg = PublishMessage {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            retain,
            dup: false,
            packet_id: None,
            user_properties: Vec::new(),
        };

        self.req_tx
            .send(ClientRequest::Publish {
                message: msg,
                ack_sender: Some(ack_tx),
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        ack_rx.await.map_err(|_| ClientError::Cancelled)?
    }

    /// Fast-path multi-packet burst publish: sends a batch of messages in one call.
    pub async fn publish_batch(&self, messages: Vec<PublishMessage>) -> Result<usize, ClientError> {
        let (ack_tx, ack_rx) = oneshot::channel();

        self.req_tx
            .send(ClientRequest::PublishBatch {
                messages,
                ack_sender: Some(ack_tx),
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        ack_rx.await.map_err(|_| ClientError::Cancelled)?
    }

    /// Sends an unreliable QUIC datagram for real-time sensor streams without head-of-line blocking.
    ///
    /// Only supported when connected over QUIC (`quic://` transport).
    pub async fn publish_datagram(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<(), ClientError> {
        let (ack_tx, ack_rx) = oneshot::channel();

        self.req_tx
            .send(ClientRequest::PublishDatagram {
                topic: topic.into(),
                payload: payload.into(),
                ack_sender: Some(ack_tx),
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        ack_rx.await.map_err(|_| ClientError::Cancelled)?
    }

    /// Subscribes to a topic filter without creating a dedicated stream channel.
    pub async fn subscribe(&self, topic: impl Into<String>, qos: QoS) -> Result<u16, ClientError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let topic_str = topic.into();

        self.req_tx
            .send(ClientRequest::Subscribe {
                topic: topic_str,
                qos,
                resp_sender: resp_tx,
                stream_sender: None,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        resp_rx.await.map_err(|_| ClientError::Cancelled)?
    }

    /// Subscribes to a topic filter and returns a dedicated [`TopicSubscription`] stream.
    ///
    /// The stream automatically matches and receives all incoming publishes matching the filter
    /// (including single-level `+` and multi-level `#` wildcards) with zero manual topic parsing.
    pub async fn subscribe_stream(
        &self,
        topic: impl Into<String>,
        qos: QoS,
    ) -> Result<TopicSubscription, ClientError> {
        let topic_str = topic.into();
        let (stream_tx, stream_rx) = mpsc::channel(128);
        let (resp_tx, resp_rx) = oneshot::channel();

        self.req_tx
            .send(ClientRequest::Subscribe {
                topic: topic_str.clone(),
                qos,
                resp_sender: resp_tx,
                stream_sender: Some(stream_tx),
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        let _ = resp_rx.await.map_err(|_| ClientError::Cancelled)??;

        Ok(TopicSubscription::new(topic_str, stream_rx))
    }

    /// Unsubscribes from a topic filter.
    pub async fn unsubscribe(&self, topic: impl Into<String>) -> Result<u16, ClientError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.req_tx
            .send(ClientRequest::Unsubscribe {
                topic: topic.into(),
                resp_sender: resp_tx,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        resp_rx.await.map_err(|_| ClientError::Cancelled)?
    }

    /// Gracefully disconnects the client from the broker.
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.req_tx
            .send(ClientRequest::Disconnect {
                resp_sender: resp_tx,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        resp_rx.await.map_err(|_| ClientError::Cancelled)?
    }

    /// Returns a watch receiver observing current connection status changes.
    pub fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status_rx.clone()
    }

    /// Returns true if the client is currently connected.
    pub fn is_connected(&self) -> bool {
        *self.status_rx.borrow() == ConnectionStatus::Connected
    }
}

/// The main entry point for constructing and spawning Tokio MQTT clients.
pub struct Client;

impl Client {
    /// Connects to the MQTT broker and spawns a background Tokio task to drive the event loop.
    ///
    /// Returns the cloneable [`AsyncClient`] handle and the driver's [`JoinHandle`].
    pub fn connect(options: ClientOptions) -> (AsyncClient, JoinHandle<()>) {
        let (req_tx, req_rx) = mpsc::channel(options.channel_capacity);
        let (status_tx, status_rx) = watch::channel(ConnectionStatus::Disconnected);

        let mut event_loop = EventLoop::new(options, req_rx, status_tx);
        let client = AsyncClient::new(req_tx, status_rx);

        let handle = tokio::spawn(async move {
            event_loop.run().await;
        });

        (client, handle)
    }

    /// Creates the client and event loop as a split pair without spawning a background task,
    /// giving the caller full manual control over the loop driver.
    pub fn new_split(options: ClientOptions) -> (AsyncClient, EventLoop) {
        let (req_tx, req_rx) = mpsc::channel(options.channel_capacity);
        let (status_tx, status_rx) = watch::channel(ConnectionStatus::Disconnected);

        let event_loop = EventLoop::new(options, req_rx, status_tx);
        let client = AsyncClient::new(req_tx, status_rx);

        (client, event_loop)
    }
}
