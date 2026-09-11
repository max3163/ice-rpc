//! Wire-level types and the conversion rules between them.
//!
//! [`Event`] is what consumers observe, [`WireEvent`] is what travels over
//! iceoryx2 (it carries the `CompleteWith` single-sample optimization), and
//! [`Sender`] is the producer side. The two conversions — [`From<Event>`] and
//! [`normalize_wire_event`] — are described here and nowhere else.
//!
//! [`From<Event>`]: From

use iceoryx2::prelude::*;
use rkyv::{Archive, Deserialize, Serialize};

use super::error::RpcError;

/// Error carried by an [`Event`].
///
/// Follows the Rx pattern: a single `error` channel, where the payload
/// distinguishes a **business** error (authored by the service) from a
/// **technical** one (raised by the framework/transport).
#[derive(Debug, Clone)]
pub enum ObservableError<E> {
    /// Business error emitted by the service.
    Business(E),
    /// Technical RPC error (transport, discovery, protocol, ...).
    Technical(RpcError),
}

impl<E> ObservableError<E> {
    /// Returns `true` when this is a technical error.
    #[inline]
    pub fn is_technical(&self) -> bool {
        matches!(self, ObservableError::Technical(_))
    }

    /// Returns `true` when this is a business error.
    #[inline]
    pub fn is_business(&self) -> bool {
        matches!(self, ObservableError::Business(_))
    }

    /// Returns the inner business error, if any.
    #[inline]
    pub fn as_business(&self) -> Option<&E> {
        match self {
            ObservableError::Business(e) => Some(e),
            ObservableError::Technical(_) => None,
        }
    }

    /// Returns the inner technical error, if any.
    #[inline]
    pub fn as_technical(&self) -> Option<&RpcError> {
        match self {
            ObservableError::Technical(e) => Some(e),
            ObservableError::Business(_) => None,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for ObservableError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObservableError::Business(e) => write!(f, "{e}"),
            ObservableError::Technical(e) => write!(f, "{e}"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for ObservableError<E> {}

/// Event emitted by an RPC stream, as observed by consumers.
///
/// This is the user-facing event type: the transport-level [`WireEvent`]
/// `CompleteWith` optimization is never exposed here. A single `Error` variant
/// carries both business and technical failures ([`ObservableError`]).
#[derive(Debug, Clone)]
pub enum Event<T, E> {
    /// Intermediate business value.
    Next(T),
    /// Normal end of the stream.
    Complete,
    /// Terminal error (business or technical).
    Error(ObservableError<E>),
}

impl<T, E> Event<T, E> {
    /// Returns `true` if this event terminates the stream.
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Event::Complete | Event::Error(_))
    }
}

/// Transport-level event carried over the wire and through the producer channel.
///
/// Internal counterpart of [`Event`]: it adds the [`WireEvent::CompleteWith`]
/// single-sample optimization used by producers. Consumers never observe it —
/// [`crate::Observable::recv`] normalizes it into [`Event`].
#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[doc(hidden)]
pub enum WireEvent<T, E> {
    /// Intermediate business value.
    Next(T),
    /// Normal end of the stream.
    Complete,
    /// Single terminal value carried by the Complete.
    CompleteWith(T),
    /// Business error.
    Error(E),
    /// Technical RPC error (e.g. incompatible version), terminal.
    RpcError(RpcError),
}

impl<T, E> WireEvent<T, E> {
    /// Returns `true` if this event terminates the stream.
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WireEvent::Complete
                | WireEvent::CompleteWith(_)
                | WireEvent::Error(_)
                | WireEvent::RpcError(_)
        )
    }
}

/// Producer-side sender of RPC events.
///
/// Producers emit through the ergonomic methods [`Sender::send_next`],
/// [`Sender::send_complete`], [`Sender::send_complete_with`] and
/// [`Sender::send_error`]. [`Sender::send_event`] is a passthrough used by the
/// transport and the `ice-rpc-rx` relays to forward any [`Event`], including
/// technical errors.
pub struct Sender<T, E> {
    /// Shared with `channel` in [`super::stream`].
    pub(crate) inner: async_channel::Sender<WireEvent<T, E>>,
}

impl<T, E> Clone for Sender<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, E> Sender<T, E> {
    /// Sends a business value.
    #[inline]
    pub async fn send_next(
        &self,
        value: T,
    ) -> Result<(), async_channel::SendError<WireEvent<T, E>>> {
        self.inner.send(WireEvent::Next(value)).await
    }

    /// Sends a normal end of stream.
    #[inline]
    pub async fn send_complete(&self) -> Result<(), async_channel::SendError<WireEvent<T, E>>> {
        self.inner.send(WireEvent::Complete).await
    }

    /// Sends a single terminal value (producer shortcut, one wire sample).
    #[inline]
    pub async fn send_complete_with(
        &self,
        value: T,
    ) -> Result<(), async_channel::SendError<WireEvent<T, E>>> {
        self.inner.send(WireEvent::CompleteWith(value)).await
    }

    /// Sends a business error.
    #[inline]
    pub async fn send_error(
        &self,
        err: E,
    ) -> Result<(), async_channel::SendError<WireEvent<T, E>>> {
        self.inner.send(WireEvent::Error(err)).await
    }

    /// Forwards any consumer [`Event`] (transport/relay passthrough).
    ///
    /// This is the only way a technical error transits: an
    /// [`ObservableError::Technical`] is mapped to [`WireEvent::RpcError`] by
    /// the [`From`] conversion described below.
    #[inline]
    pub async fn send_event(
        &self,
        event: Event<T, E>,
    ) -> Result<(), async_channel::SendError<WireEvent<T, E>>> {
        self.inner.send(event.into()).await
    }

    /// Non-blocking variant of [`Sender::send_next`].
    #[inline]
    pub fn try_send_next(
        &self,
        value: T,
    ) -> Result<(), async_channel::TrySendError<WireEvent<T, E>>> {
        self.inner.try_send(WireEvent::Next(value))
    }

    /// Non-blocking variant of [`Sender::send_complete`].
    #[inline]
    pub fn try_send_complete(&self) -> Result<(), async_channel::TrySendError<WireEvent<T, E>>> {
        self.inner.try_send(WireEvent::Complete)
    }

    /// Non-blocking variant of [`Sender::send_complete_with`].
    #[inline]
    pub fn try_send_complete_with(
        &self,
        value: T,
    ) -> Result<(), async_channel::TrySendError<WireEvent<T, E>>> {
        self.inner.try_send(WireEvent::CompleteWith(value))
    }

    /// Non-blocking variant of [`Sender::send_error`].
    #[inline]
    pub fn try_send_error(
        &self,
        err: E,
    ) -> Result<(), async_channel::TrySendError<WireEvent<T, E>>> {
        self.inner.try_send(WireEvent::Error(err))
    }

    /// Non-blocking variant of [`Sender::send_event`].
    #[inline]
    pub fn try_send_event(
        &self,
        event: Event<T, E>,
    ) -> Result<(), async_channel::TrySendError<WireEvent<T, E>>> {
        self.inner.try_send(event.into())
    }

    /// Forwards a raw transport event (relay passthrough, consumer side).
    ///
    /// Used by the generated client to relay an IPC sample to the consumer
    /// channel **without re-encoding it**: the [`WireEvent::CompleteWith`]
    /// single-sample optimization therefore survives as a single channel
    /// message instead of being split into `Next` + `Complete`.
    #[doc(hidden)]
    #[inline]
    pub fn try_send_wire(
        &self,
        event: WireEvent<T, E>,
    ) -> Result<(), async_channel::TrySendError<WireEvent<T, E>>> {
        self.inner.try_send(event)
    }
}
/// Converts a user-facing [`Event`] into its transport representation.
///
/// This is the **only** place describing the mapping rule: a business error
/// becomes [`WireEvent::Error`], a technical one becomes [`WireEvent::RpcError`].
impl<T, E> From<Event<T, E>> for WireEvent<T, E> {
    fn from(event: Event<T, E>) -> Self {
        match event {
            Event::Next(v) => WireEvent::Next(v),
            Event::Complete => WireEvent::Complete,
            Event::Error(ObservableError::Business(e)) => WireEvent::Error(e),
            Event::Error(ObservableError::Technical(e)) => WireEvent::RpcError(e),
        }
    }
}

/// Normalizes a transport [`WireEvent`] into the user-facing form.
///
/// Returns the event to yield **now**, plus an optional **follow-up** event: the
/// [`WireEvent::CompleteWith`] single-sample optimization expands into
/// `Next(v)` followed by `Complete`. This is the only place describing the
/// expansion, shared by [`crate::Observable::recv`] and the
/// [`futures_lite::Stream`] implementation of `crate::Observable`.
pub(crate) fn normalize_wire_event<T, E>(
    event: WireEvent<T, E>,
) -> (Event<T, E>, Option<Event<T, E>>) {
    match event {
        WireEvent::Next(v) => (Event::Next(v), None),
        WireEvent::Complete => (Event::Complete, None),
        WireEvent::CompleteWith(v) => (Event::Next(v), Some(Event::Complete)),
        WireEvent::Error(e) => (Event::Error(ObservableError::Business(e)), None),
        WireEvent::RpcError(e) => (Event::Error(ObservableError::Technical(e)), None),
    }
}
/// Discriminant of the RPC event type carried in the [`RpcHeader`].
///
/// `#[repr(C)]` is required by `ZeroCopySend`. The values are fixed.
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, ZeroCopySend, Default, Archive, Serialize, Deserialize,
)]
pub enum EventKind {
    /// Request emitted by the client (non-terminal).
    #[default]
    Request = 0,
    /// Intermediate event carrying a business value.
    Next = 1,
    /// Normal end of the stream (terminal).
    Complete = 2,
    /// Business error (terminal).
    Error = 3,
}

impl EventKind {
    /// Returns `true` if this event terminates the stream.
    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(self, EventKind::Complete | EventKind::Error)
    }
}
