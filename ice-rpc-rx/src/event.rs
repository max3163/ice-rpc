//! The event vocabulary of a stream, and its producer side.
//!
//! [`Event`] is what a consumer observes, [`ObservableError`] is the single
//! error type it may carry, and [`Sender`] is the producer side of a local
//! channel. Nothing here knows about the wire: framing and serialization belong
//! to the transport, which converts these events to its own representation.

use crate::error::RpcError;

/// The single error type of the whole streaming API.
///
/// Follows the Rx pattern: a single `error` channel whose payload distinguishes
/// a **business** error (authored by the service) from a **technical** one
/// (raised by the framework/transport). [`ObservableError::Empty`] is a
/// pull-side artefact and never travels over the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservableError<E> {
    /// Business error emitted by the service.
    Business(E),
    /// Technical RPC error (transport, discovery, protocol, ...).
    Technical(RpcError),
    /// The stream ended without emitting any value.
    Empty,
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
            ObservableError::Technical(_) | ObservableError::Empty => None,
        }
    }

    /// Returns the inner technical error, if any.
    #[inline]
    pub fn as_technical(&self) -> Option<&RpcError> {
        match self {
            ObservableError::Technical(e) => Some(e),
            ObservableError::Business(_) | ObservableError::Empty => None,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for ObservableError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObservableError::Business(e) => write!(f, "{e}"),
            ObservableError::Technical(e) => write!(f, "{e}"),
            ObservableError::Empty => write!(f, "stream ended without a value"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for ObservableError<E> {}

/// Event emitted by a stream, as observed by consumers.
///
/// This is the user-facing event type: a single `Error` variant carries both
/// business and technical failures ([`ObservableError`]). Whatever single-sample
/// optimization a source applies internally is normalized away before a consumer
/// sees it (see [`Observable::recv`](crate::Observable::recv)).
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Producer-side sender of stream events.
///
/// Producers emit through the ergonomic methods [`Sender::send_next`],
/// [`Sender::send_complete`], [`Sender::send_complete_with`] and
/// [`Sender::send_error`]. [`Sender::send_event`] is a passthrough used by the
/// transport relays to forward any [`Event`], including technical errors.
///
/// The channel carries [`Event`]s, not wire samples: the wire vocabulary belongs
/// to the transport, which is the only layer that frames a sample, stamps its
/// `EventKind` and serializes it. The single-sample optimization is preserved
/// across that boundary by [`Sender::send_complete_with`], which enqueues
/// `Next(value)` then `Complete`: the transport bridge folds the pair back into
/// one `CompleteWith` sample.
pub struct Sender<T, E> {
    /// Shared with `channel` in [`super::stream`].
    pub(crate) inner: async_channel::Sender<Event<T, E>>,
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
    pub async fn send_next(&self, value: T) -> Result<(), async_channel::SendError<Event<T, E>>> {
        self.inner.send(Event::Next(value)).await
    }

    /// Sends a normal end of stream.
    #[inline]
    pub async fn send_complete(&self) -> Result<(), async_channel::SendError<Event<T, E>>> {
        self.inner.send(Event::Complete).await
    }

    /// Sends a single terminal value (producer shortcut, one wire sample).
    ///
    /// Enqueues `Next(value)` then `Complete`. The pair travels as two channel
    /// events and is folded back into a single wire sample by the transport
    /// bridge, so the sample a subscriber receives is unchanged. A caller that
    /// uses this on a **bounded** channel must leave room for two events.
    #[inline]
    pub async fn send_complete_with(
        &self,
        value: T,
    ) -> Result<(), async_channel::SendError<Event<T, E>>> {
        self.inner.send(Event::Next(value)).await?;
        self.inner.send(Event::Complete).await
    }

    /// Sends a business error.
    #[inline]
    pub async fn send_error(&self, err: E) -> Result<(), async_channel::SendError<Event<T, E>>> {
        self.inner
            .send(Event::Error(ObservableError::Business(err)))
            .await
    }

    /// Forwards any consumer [`Event`] (transport/relay passthrough).
    ///
    /// The only way a technical error transits.
    #[inline]
    pub async fn send_event(
        &self,
        event: Event<T, E>,
    ) -> Result<(), async_channel::SendError<Event<T, E>>> {
        self.inner.send(event).await
    }

    /// Non-blocking variant of [`Sender::send_next`].
    #[inline]
    pub fn try_send_next(&self, value: T) -> Result<(), async_channel::TrySendError<Event<T, E>>> {
        self.inner.try_send(Event::Next(value))
    }

    /// Non-blocking variant of [`Sender::send_complete`].
    #[inline]
    pub fn try_send_complete(&self) -> Result<(), async_channel::TrySendError<Event<T, E>>> {
        self.inner.try_send(Event::Complete)
    }

    /// Non-blocking variant of [`Sender::send_complete_with`].
    ///
    /// Both events go through the same non-blocking send, so a channel with room
    /// for one event only leaves a `Next` queued when the `Complete` is rejected:
    /// a caller that swallows that error hands its consumer an unterminated
    /// stream. Treat any failure here as fatal for the response.
    #[inline]
    pub fn try_send_complete_with(
        &self,
        value: T,
    ) -> Result<(), async_channel::TrySendError<Event<T, E>>> {
        self.inner.try_send(Event::Next(value))?;
        self.inner.try_send(Event::Complete)
    }

    /// Non-blocking variant of [`Sender::send_error`].
    #[inline]
    pub fn try_send_error(&self, err: E) -> Result<(), async_channel::TrySendError<Event<T, E>>> {
        self.inner
            .try_send(Event::Error(ObservableError::Business(err)))
    }

    /// Non-blocking variant of [`Sender::send_event`].
    #[inline]
    pub fn try_send_event(
        &self,
        event: Event<T, E>,
    ) -> Result<(), async_channel::TrySendError<Event<T, E>>> {
        self.inner.try_send(event)
    }
}
