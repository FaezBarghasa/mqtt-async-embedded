# Wire Protocol Schemas & Binary Packet Layouts

This document details the binary frame formats, variable header schemas, control packet representations, and error code mappings for `mqtt-async-embedded`.

---

## 1. MQTT Fixed Header Binary Structure

All MQTT control packets share a 2-byte minimum Fixed Header:

```
 Bit   |  7  |  6  |  5  |  4  |  3  |  2  |  1  |  0  |
Byte 1 | MQTT Control Packet Type |     Flags / QoS     |
Byte 2+| Remaining Length (1 - 4 bytes Variable Byte Integer)
```

### Packet Type Enumeration (`ControlPacketType`)

| Type Value | Name | Description | Direction |
| :--- | :--- | :--- | :--- |
| `0x01` | **`CONNECT`** | Client request to connect to Broker | Client → Broker |
| `0x02` | **`CONNACK`** | Connect Acknowledgment | Broker → Client |
| `0x03` | **`PUBLISH`** | Publish Message | Client ↔ Broker |
| `0x04` | **`PUBACK`** | Publish Acknowledgment (QoS 1) | Client ↔ Broker |
| `0x05` | **`PUBREC`** | Publish Received (QoS 2 delivery part 1) | Broker → Client |
| `0x06` | **`PUBREL`** | Publish Release (QoS 2 delivery part 2) | Broker → Client |
| `0x07` | **`PUBCOMP`** | Publish Complete (QoS 2 delivery part 3) | Broker → Client |
| `0x08` | **`SUBSCRIBE`** | Subscribe Request | Client → Broker |
| `0x09` | **`SUBACK`** | Subscribe Acknowledgment | Broker → Client |
| `0x0A` | **`UNSUBSCRIBE`** | Unsubscribe Request | Client → Broker |
| `0x0B` | **`UNSUBACK`** | Unsubscribe Acknowledgment | Broker → Client |
| `0x0C` | **`PINGREQ`** | PING Request (Keep-alive) | Client → Broker |
| `0x0D` | **`PINGRESP`** | PING Response | Broker → Client |
| `0x0E` | **`DISCONNECT`** | Disconnect Notification | Client → Broker |

---

## 2. In-Memory Rust Packet Schemas (`src/packet.rs`)

### 2.1. Quality of Service (QoS) Level
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}
```

### 2.2. Last Will and Testament Schema (`Will<'a>`)
```rust
pub struct Will<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub qos: QoS,
    pub retain: bool,
    pub properties: Vec<Property<'a>, 8>,
}
```

### 2.3. CONNECT Packet Schema (`Connect<'a>`)
```rust
pub struct Connect<'a> {
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: &'a str,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub will: Option<Will<'a>>,
    pub properties: Vec<Property<'a>, 8>,
}
```

### 2.4. CONNACK Packet Schema (`ConnAck<'a>`)
```rust
pub struct ConnAck<'a> {
    pub session_present: bool,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}
```

### 2.5. PUBLISH Packet Schema (`Publish<'a>`)
```rust
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a str,
    pub packet_id: Option<u16>,
    pub payload: &'a [u8],
    pub properties: Vec<Property<'a>, 8>,
}
```

### 2.6. SUBSCRIBE & SUBACK Schemas
```rust
pub struct Subscribe<'a> {
    pub packet_id: u16,
    pub topics: Vec<(&'a str, QoS), 8>,
    pub properties: Vec<Property<'a>, 8>,
}

pub struct SubAck<'a> {
    pub packet_id: u16,
    pub reason_codes: Vec<u8, 8>,
    pub properties: Vec<Property<'a>, 8>,
}
```

### 2.7. UNSUBSCRIBE & UNSUBACK Schemas
```rust
pub struct Unsubscribe<'a> {
    pub packet_id: u16,
    pub topics: Vec<&'a str, 8>,
    pub properties: Vec<Property<'a>, 8>,
}

pub struct UnsubAck<'a> {
    pub packet_id: u16,
    pub reason_codes: Vec<u8, 8>,
    pub properties: Vec<Property<'a>, 8>,
}
```

---

## 3. Variable Byte Integer Encoding (MQTT Length Schema)

Remaining Length fields use a variable byte encoding scheme where 7 bits carry payload length data and bit 8 indicates continuation:

| Length Bytes | Minimum Value | Maximum Value | Max Representation |
| :--- | :--- | :--- | :--- |
| 1 byte | `0` (`0x00`) | `127` (`0x7F`) | 127 B |
| 2 bytes | `128` (`0x80 0x01`) | `16383` (`0xFF 0x7F`) | 16.383 KB |
| 3 bytes | `16384` | `2097151` | 2.097 MB |
| 4 bytes | `2097152` | `268435455` | 256.0 MB |

---

## 4. Error Mapping Schema (`src/error.rs`)

```rust
pub enum MqttError<T> {
    Transport(T),
    Protocol(ProtocolError),
    ConnectionRefused(ConnectReasonCode),
    NotConnected,
    BufferTooSmall,
    Timeout,
    BatchCapacityExceeded,
    QuicError(QuicErrorKind),
}
```

```rust
pub enum ProtocolError {
    InvalidPacketType(u8),
    InvalidResponse,
    MalformedPacket,
    IncompletePacket,
    PayloadTooLarge,
    InvalidUtf8String,
    TooManyProperties,
    InvalidTopic,
    UnsupportedQoS,
}
```

