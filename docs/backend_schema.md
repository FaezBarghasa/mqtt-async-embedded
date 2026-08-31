# Wire Protocol Schemas & Binary Packet Layouts

Binary frame layouts, control packet types, variable headers, and error mappings for `mqtt-async-embedded`.

---

## 1. Fixed Header Binary Structure

Every MQTT control packet starts with a 2-byte minimum header:

```
 Bit   |  7  |  6  |  5  |  4  |  3  |  2  |  1  |  0  |
Byte 1 | MQTT Control Packet Type |     Flags / QoS     |
Byte 2+| Remaining Length (1 - 4 bytes Variable Byte Integer)
```

### Control Packet Types

| Value | Type | Direction | Description |
| :--- | :--- | :--- | :--- |
| `0x01` | **`CONNECT`** | Client → Broker | Connection request |
| `0x02` | **`CONNACK`** | Broker → Client | Connection acknowledge |
| `0x03` | **`PUBLISH`** | Client ↔ Broker | Message transfer |
| `0x04` | **`PUBACK`** | Client ↔ Broker | QoS 1 publish acknowledge |
| `0x05` | **`PUBREC`** | Broker → Client | QoS 2 publish received |
| `0x06` | **`PUBREL`** | Broker → Client | QoS 2 publish release |
| `0x07` | **`PUBCOMP`** | Broker → Client | QoS 2 publish complete |
| `0x08` | **`SUBSCRIBE`** | Client → Broker | Subscription request |
| `0x09` | **`SUBACK`** | Broker → Client | Subscription acknowledge |
| `0x0A` | **`UNSUBSCRIBE`** | Client → Broker | Unsubscribe request |
| `0x0B` | **`UNSUBACK`** | Broker → Client | Unsubscribe acknowledge |
| `0x0C` | **`PINGREQ`** | Client → Broker | Keep-alive heartbeat request |
| `0x0D` | **`PINGRESP`** | Broker → Client | Keep-alive heartbeat response |
| `0x0E` | **`DISCONNECT`** | Client → Broker | Disconnect notification |

---

## 2. In-Memory Rust Structs (`src/packet.rs`)

### 2.1. Quality of Service (`QoS`)
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}
```

### 2.2. Core Packet Types
```rust
// Last Will and Testament
pub struct Will<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub qos: QoS,
    pub retain: bool,
    pub properties: Vec<Property<'a>, 8>,
}

// CONNECT / CONNACK
pub struct Connect<'a> {
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: &'a str,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub will: Option<Will<'a>>,
    pub properties: Vec<Property<'a>, 8>,
}

pub struct ConnAck<'a> {
    pub session_present: bool,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

// PUBLISH
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a str,
    pub packet_id: Option<u16>,
    pub payload: &'a [u8],
    pub properties: Vec<Property<'a>, 8>,
}

// SUBSCRIBE / SUBACK
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

// UNSUBSCRIBE / UNSUBACK
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

### 2.3. Streaming Types (`src/client.rs`)
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamMode {
    #[default]
    Standard,
    RealTimeStreaming,
}

pub struct MqttStreamWriter<'c, T: MqttTransport> {
    transport: &'c mut T,
    remaining_bytes: usize,
    total_bytes: usize,
}
```

---

## 3. Variable Byte Integer Length Table

7 bits data per byte. Bit 7 is continuation flag.

| Bytes | Value Range | Max Capacity |
| :--- | :--- | :--- |
| **1 byte** | `0` (`0x00`) to `127` (`0x7F`) | 127 B |
| **2 bytes** | `128` (`0x80 0x01`) to `16,383` (`0xFF 0x7F`) | 16.38 KB |
| **3 bytes** | `16,384` to `2,097,151` | 2.09 MB |
| **4 bytes** | `2,097,152` to `268,435,455` | 256.0 MB |

---

## 4. Error Types (`src/error.rs`)

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
