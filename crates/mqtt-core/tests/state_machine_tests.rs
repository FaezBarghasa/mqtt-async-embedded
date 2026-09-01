//! # State Machine & Inflight Tests
//!
//! Tests the deterministic pure protocol state machine and collision detection logic in mqtt-core.

use mqtt_core::error::ProtocolError;
use mqtt_core::inflight::InflightQueue;
use mqtt_core::state::{ConnState, StateAction, StateEvent, transition};

use mqtt_packet::QoS;

#[test]
fn test_state_machine_connection_lifecycle() {
    let mut state = ConnState::Disconnected;

    // Connect requested
    let (next, action) = transition(state, StateEvent::ConnectRequested).unwrap();
    assert_eq!(next, ConnState::Connecting);
    assert_eq!(action, StateAction::None);
    state = next;

    // Transport connected -> sends CONNECT
    let (next, action) = transition(state, StateEvent::TransportConnected).unwrap();
    assert_eq!(next, ConnState::WaitingForConnAck);
    assert_eq!(action, StateAction::SendConnect);
    state = next;

    // CONNACK received -> Connected
    let (next, action) = transition(
        state,
        StateEvent::ConnAckReceived {
            session_present: false,
        },
    )
    .unwrap();
    assert_eq!(next, ConnState::Connected);
    assert_eq!(
        action,
        StateAction::NotifyConnected {
            session_present: false
        }
    );
    state = next;

    // Keepalive ping
    let (next, action) = transition(state, StateEvent::KeepAliveExpired).unwrap();
    assert_eq!(next, ConnState::Connected);
    assert_eq!(action, StateAction::SendPing);
    state = next;

    // Disconnect requested
    let (next, action) = transition(state, StateEvent::DisconnectRequested).unwrap();
    assert_eq!(next, ConnState::Disconnecting);
    assert_eq!(action, StateAction::SendDisconnect);
    state = next;

    // Transport closed
    let (next, action) = transition(state, StateEvent::TransportClosed).unwrap();
    assert_eq!(next, ConnState::Disconnected);
    assert_eq!(action, StateAction::NotifyDisconnected);
}

#[test]
fn test_state_machine_reconnect_on_unexpected_drop() {
    let state = ConnState::Connected;
    let (next, action) = transition(state, StateEvent::TransportClosed).unwrap();
    assert_eq!(next, ConnState::Reconnecting { attempt: 1 });
    assert_eq!(action, StateAction::ScheduleReconnect { attempt: 1 });

    // Repeated drop during reconnect increases attempt
    let (next, action) = transition(next, StateEvent::TransportClosed).unwrap();
    assert_eq!(next, ConnState::Reconnecting { attempt: 2 });
    assert_eq!(action, StateAction::ScheduleReconnect { attempt: 2 });
}

#[test]
fn test_state_machine_invalid_transition() {
    let state = ConnState::Disconnected;
    let err = transition(state, StateEvent::KeepAliveExpired).unwrap_err();
    assert_eq!(err, ProtocolError::StateMismatch);
}

#[test]
fn test_inflight_queue_and_collision_detection() {
    let mut queue: InflightQueue<8> = InflightQueue::new();
    assert!(queue.is_empty());

    // Push packet 1
    assert!(queue.push(1, QoS::AtLeastOnce).is_ok());
    assert_eq!(queue.len(), 1);

    // Collision detection: pushing packet 1 again fails
    assert_eq!(queue.push(1, QoS::AtLeastOnce), Err(1));

    // Push packet 2
    assert!(queue.push(2, QoS::ExactlyOnce).is_ok());
    assert_eq!(queue.len(), 2);

    // Advance packet 2 to PubRelSent
    assert!(queue.mark_pubrel_sent(2));

    // Acknowledge packet 1
    assert!(queue.acknowledge(1));
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.last_acked_id(), Some(1));

    // Now packet 1 can be pushed again without collision
    assert!(queue.push(1, QoS::AtLeastOnce).is_ok());
}
