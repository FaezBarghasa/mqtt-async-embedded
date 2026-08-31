# Application Flow & Execution Lifecycle

State machines, packet lifecycles, and session recovery mechanics for `mqtt-async-embedded`.

---

## 1. Dual-Engine Architecture Flow

```mermaid
flowchart TD
    subgraph Init [1. Initialization]
        A1[Embedded: MqttOptions + MqttTransport] --> B1[MqttClient<T, MAX_TOPICS, BUF_SIZE>]
        A2[Tokio: ClientOptions URI] --> B2[Client::connect / Client::new_split]
    end

    subgraph EmbeddedFlow [2. Embedded no_std Loop]
        B1 --> C1[client.connect]
        C1 --> D1[client.poll loop]
        D1 --> E1[Chunk Streaming / Burst Batching]
    end

    subgraph TokioFlow [3. Tokio Background Driver]
        B2 --> C2[AsyncClient Handle]
        B2 --> D2[EventLoop Driver]
        D2 --> E2{Connection Active?}
        E2 -- Yes --> F2[Multiplex Requests & Packets]
        E2 -- Disconnect --> G2[Reconnect Backoff & Recovery]
        G2 --> H2[1. Resend In-Flight DUP=true]
        G2 --> I2[2. Restore Subscriptions]
        G2 --> J2[3. Drain Offline Queue]
        J2 --> E2
    end
```

---

## 2. Multi-Threaded Data Stream & Recovery Pipeline

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
    Prod->>Journal: Buffer chunk into sliding window
    Prod->>EL: ClientRequest::Publish
    EL->>Broker: PUBLISH (wire packet)
    Broker->>EL: Forward PUBLISH to subscriber
    EL->>Cons: TopicRouter::dispatch
    Cons->>Cons: Check seq_id & reorder buffer
    Cons-->>App: recv_ordered() yields in-order chunk

    Note over EL,Broker: Network Disconnection
    Broker--xEL: Connection Dropped
    EL->>EL: Exponential backoff + jitter
    EL->>Broker: Reconnect & CONNACK
    Prod->>EL: replay_recovery_journal()
    EL->>Broker: PUBLISH (DUP=true, unacked chunks)
    Broker->>Cons: Deliver replayed chunks
    Cons->>Cons: Deduplicate old seq_id & fill gaps
```

---

## 3. Operational Lifecycles

### 3.1. Reconnect & Recovery Sequence

1. **Disconnect Event**: Socket error or heartbeat timeout triggers `ConnectionStatus::Disconnected`.
2. **Backoff Calculation**: `ReconnectPolicy::compute_delay(attempt)` sets next retry interval (exponential + jitter).
3. **Transport Re-dial**: Reconnects over TCP, TLS, QUIC, Unix socket, or Windows Named Pipe.
4. **Resubscription**: Replays all active topics in `active_subscriptions` via batch `SUBSCRIBE`.
5. **In-Flight Retransmit**: Unacknowledged QoS 1 & 2 messages are marked `dup = true` and re-sent immediately.
6. **Offline Queue Flush**: Messages queued during outage are flushed based on `DropStrategy` (`DropOldest`, `ErrorOnFull`, `Block`).

---

### 3.2. Topic Stream Router

- **Structure**: Trie prefix tree matching exact paths, single-level (`+`), and multi-level (`#`) wildcards.
- **Lookup Time**: $O(k)$ where $k$ is topic path depth.
- **Zero-Copy Routing**: Distributes cloned `PublishMessage` handles (`bytes::Bytes`) to matching channels.
- **Auto-Pruning**: Automatically cleans up closed receiver channels to eliminate memory leaks.
