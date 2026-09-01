//! # Pure Protocol State Machine
//!
//! Provides deterministic, I/O-free state transitions for MQTT client connections.

use crate::error::ProtocolError;

/// Explicit connection states for the MQTT client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ConnState {
    /// Client is idle and disconnected from any broker.
    Disconnected,
    /// Physical transport connection is being established.
    Connecting,
    /// `CONNECT` packet was sent, awaiting `CONNACK` from the broker.
    WaitingForConnAck,
    /// Connection handshake completed successfully; ready for normal pub/sub.
    Connected,
    /// Graceful `DISCONNECT` in progress.
    Disconnecting,
    /// Connection lost; waiting for backoff timer to reconnect.
    Reconnecting { attempt: u32 },
}

/// Incoming stimulus driving state transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateEvent {
    ConnectRequested,
    TransportConnected,
    ConnAckReceived { session_present: bool },
    ConnAckRejected,
    DisconnectRequested,
    TransportClosed,
    KeepAliveExpired,
}

/// Actions produced by a state transition for the I/O driver to execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateAction {
    None,
    SendConnect,
    SendPing,
    SendDisconnect,
    NotifyConnected { session_present: bool },
    NotifyDisconnected,
    ScheduleReconnect { attempt: u32 },
}

/// Pure state machine transition function without I/O side effects.
pub fn transition(
    state: ConnState,
    event: StateEvent,
) -> Result<(ConnState, StateAction), ProtocolError> {
    match (state, event) {
        (ConnState::Disconnected, StateEvent::ConnectRequested) => {
            Ok((ConnState::Connecting, StateAction::None))
        }
        (ConnState::Connecting, StateEvent::TransportConnected) => {
            Ok((ConnState::WaitingForConnAck, StateAction::SendConnect))
        }
        (ConnState::WaitingForConnAck, StateEvent::ConnAckReceived { session_present }) => Ok((
            ConnState::Connected,
            StateAction::NotifyConnected { session_present },
        )),
        (ConnState::WaitingForConnAck, StateEvent::ConnAckRejected) => {
            Ok((ConnState::Disconnected, StateAction::NotifyDisconnected))
        }
        (ConnState::Connected, StateEvent::KeepAliveExpired) => {
            Ok((ConnState::Connected, StateAction::SendPing))
        }
        (ConnState::Connected, StateEvent::DisconnectRequested) => {
            Ok((ConnState::Disconnecting, StateAction::SendDisconnect))
        }
        (ConnState::Disconnecting, StateEvent::TransportClosed) => {
            Ok((ConnState::Disconnected, StateAction::NotifyDisconnected))
        }
        (ConnState::Connected, StateEvent::TransportClosed)
        | (ConnState::WaitingForConnAck, StateEvent::TransportClosed) => Ok((
            ConnState::Reconnecting { attempt: 1 },
            StateAction::ScheduleReconnect { attempt: 1 },
        )),
        (ConnState::Reconnecting { .. }, StateEvent::ConnectRequested) => {
            Ok((ConnState::Connecting, StateAction::None))
        }

        (ConnState::Reconnecting { attempt }, StateEvent::TransportClosed) => {
            let next_attempt = attempt.saturating_add(1);
            Ok((
                ConnState::Reconnecting {
                    attempt: next_attempt,
                },
                StateAction::ScheduleReconnect {
                    attempt: next_attempt,
                },
            ))
        }
        _ => Err(ProtocolError::StateMismatch),
    }
}
