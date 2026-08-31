# Application Flow & Execution Lifecycle

State machine, asynchronous control flow, packet exchange lifecycles, and session data recovery flows for `mqtt-async-embedded`.

---

## 1. High-Level Dual-Engine Architecture Flow

```mermaid
flowchart TD
    subgraph Initialization [Client Initialization]
        A1[Embedded: MqttOptions + MqttTransport] --> B1[Create MqttClient<T, MAX_TOPICS, BUF_SIZE>]
        A2[Tokio: ClientOptions URI] --> B2[Client::connect / Client::new_split]
    end

    subgraph EmbeddedLoop [Embedded no_std Event Loop]
        B1 --> C1[client.connect]
        C1 --> D1[client.poll loop]
        D1 --> E1[Zero-RAM chunk streaming / Burst batching]
    end

    subgraph TokioLoop [Tokio Background Driver & Recovery]
        B2 --> C2[AsyncClient Handle]
        B2 --> D2[EventLoop Background Driver]
        D2 --> E2{Connection Active?}
        E2 -- Yes --> F2[Multiplex Requests & Packets]
        E2 -- Network Drop --> G2[Data Recovery & Reconnect Backoff]
        G2 --> H2[Resend Unacked Inflight DUP=true]
        G2 --> I2[Restore Active Subscriptions]
        G2 --> J2[Drain Offline Queue Buffer]
        J2 --> E2
    end
```

---

## 2. Multi-Threaded Data Stream & Recovery Flow

```mermaid
sequenceDiagram
    autonumber
    participant App as Worker Threads
    participant Prod as DataStreamProducer
    participant Journal as Sliding Recovery Journal
    participant EL as EventLoop Driver
    participant Broker as MQTT Broker
    participant Cons as DataStreamConsumer

    App->>Prod: send(payload)
    Prod->>Prod: Atomic fetch_add(seq_id) & timestamp
    Prod->>Journal: Buffer chunk (sliding window)
    Prod->>EL: ClientRequest::Publish
    EL->>Broker: PUBLISH (wire chunk)
    Broker->>EL: Forward PUBLISH to subscribers
    EL->>Cons: TopicRouter::dispatch
    Cons->>Cons: Check seq_id & Reorder Buffer
    Cons-->>App: recv_ordered() yields ordered chunk

    Note over EL,Broker: Network Disconnect Occurs
    Broker--xEL: Connection Dropped
    EL->>EL: Enter Reconnect Backoff
    EL->>Broker: Reconnect & Handshake (CONNACK)
    Prod->>EL: replay_recovery_journal()
    EL->>Broker: PUBLISH (DUP=true, unacked chunks)
    Broker->>Cons: Deliver replayed recovery chunks
    Cons->>Cons: Deduplicate old seq_id & emit missing gaps
```

---

## 3. Core Operational Lifecycles

### 3.1. Reconnection & Data Recovery Engine
1. **Detection**: Any I/O error or abrupt disconnect triggers `ConnectionStatus::Disconnected`.
2. **Backoff**: `ReconnectPolicy::compute_delay(attempt)` computes exponential backoff with jitter.
3. **Transport Reconnection**: Reconnects over configured target (TCP, TLS, QUIC, Unix socket, or Windows Named Pipe).
4. **Subscription Restoration**: All active subscriptions in `active_subscriptions` map are re-subscribed with broker `SUBSCRIBE` packets.
5. **In-Flight Retransmission**: Unacknowledged QoS 1 & 2 messages are re-encoded with `dup = true` and flushed to the network.
6. **Offline Queue Drain**: Messages buffered in the offline queue during the outage are drained according to `DropStrategy`.

### 3.2. Topic-Filtered Stream Routing
1. Calling `client.subscribe_stream("sensors/+/telemetry", qos)` registers an `mpsc::Sender<PublishMessage>` in the `TopicRouter` trie.
2. Inbound packets are matched against the trie nodes in $O(k)$ time where $k$ is the topic depth.
3. Matching streams receive zero-copy cloned `PublishMessage` handles.
4. If a subscriber drops its stream, the router detects closed channels and prunes dead nodes automatically.
