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
| `0x08` | **`SUBSCRIBE`** | Subscribe Request | Client → Broker |
| `0x09` | **`SUBACK`** | Subscribe Acknowledgment | Broker → Client |
| `0x0C` | **`PINGREQ`** | PING Request (Keep-alive) | Client → Broker |
| `0x0D` | **`PINGRESP`** | PING Response | Broker → Client |
| `0x0E` | **`DISCONNECT`** | Disconnect Notification | Client → Broker |

---

## 2. In-Memory Rust Packet Schemas (`src/packet.rs`)

### 2.1. Quality of Service (QoS) Level
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}
```

### 2.2. CONNECT Packet Schema (`Connect<'a>`)
```rust
pub struct Connect<'a> {
    pub client_id: &'a str,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
}
```
**Binary Variable Header Flags**:
- Bit 1: Clean Session / Clean Start flag
- Bit 2: Will Flag
- Bits 3-4: Will QoS
- Bit 5: Will Retain
- Bit 6: Password Flag
- Bit 7: User Name Flag

### 2.3. CONNACK Packet Schema (`ConnAck`)
```rust
pub struct ConnAck {
    pub session_present: bool,
    pub reason_code: u8,
}
```
**Reason Codes (v3.1.1 / v5)**:
- `0x00`: Connection Accepted
- `0x01`: Unacceptable Protocol Version
- `0x02`: Identifier Rejected
- `0x03`: Server Unavailable
- `0x04`: Bad User Name or Password
- `0x05`: Not Authorized

### 2.4. PUBLISH Packet Schema (`Publish<'a>`)
```rust
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a str,
    pub packet_id: Option<u16>,
    pub payload: &'a [u8],
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
pub enum MqttError<E> {
    Transport(E),
    Protocol(ProtocolError),
    ConnectionRefused(u8),
    NotConnected,
    BufferTooSmall,
    Timeout,
}
```

```rust
pub enum ProtocolError {
    InvalidHeader,
    InvalidLength,
    InvalidString,
    InvalidResponse,
    UnsupportedVersion,
}
```
