# ADR 0004: Smart QUIC-to-TCP/TLS Fallback Strategy

## Status
Accepted

## Context
MQTT over QUIC (HTTP/3 transport) offers significant benefits for IoT and mobile clients:
- 0-RTT connection resumption
- Elimination of Head-of-Line (HoL) blocking across topic streams
- Connection migration across Wi-Fi and Cellular network switches
- Unreliable datagram support for high-frequency telemetry and video

However, enterprise firewalls, cellular carrier NATs, and legacy middleboxes frequently block outgoing UDP traffic on arbitrary ports.

## Decision
Introduce `SmartTransport` in `mqtt-tokio`:
1. The client attempts a low-latency QUIC handshake to the target endpoint.
2. If the QUIC connection encounters a timeout, connection refusal, or ICMP port unreachable error, `SmartTransport` automatically falls back to standard TCP / TLS on port 8883 / 1883 with zero user intervention.
3. Session state and queued offline messages are preserved across fallback attempts.

## Consequences
- **Positive**:
  - Delivers cutting-edge QUIC performance wherever supported.
  - Guaranteed connectivity even behind strict corporate firewalls and hostile network environments.
  - Transparent developer experience requiring no manual fallback boilerplates.
- **Negative**:
  - Initial connection attempt behind a strict UDP drop firewall incurs a brief handshake timeout penalty before falling back to TCP.
