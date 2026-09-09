//! Composable operators for [`ice_rpc::Stream`].
//!
//! The [`RxStreamExt`] trait extends any poll-based stream of [`ice_rpc::Event`]
//! with the classic reactive operators. Operators are implemented as pull-based
//! combinators: they wrap the source and implement `futures_lite::Stream`, so a
//! pipeline `take(filter(map(source)))` pulls events through a call stack with
//! no intermediate channel allocation and no spawned task.
//!
//! Available operators:
//! - [`map`](RxStreamExt::map) — transforms every `Next` value;
//! - [`filter`](RxStreamExt::filter) — keeps only the `Next` values matching a
//!   predicate;
//! - [`take`](RxStreamExt::take) — emits at most `n` `Next` values then
//!   completes;
//! - [`skip`](RxStreamExt::skip) — ignores the first `n` values;
//! - [`first`](RxStreamExt::first) — emits only the first `Next` value;
//! - [`first_with`](RxStreamExt::first_with) — emits the first `Next` value
//!   matching a predicate;
//! - [`start_with`](RxStreamExt::start_with) — prefixes the stream with a value;
//! - [`map_err`](RxStreamExt::map_err) — maps the error type to another one;
//! - [`scan`](RxStreamExt::scan) — emits a running accumulator state;
//! - [`tap`](RxStreamExt::tap) — runs a side effect per value;
//! - [`finalize`](RxStreamExt::finalize) — runs a callback once at termination;
//! - [`catch_error`](RxStreamExt::catch_error) — replaces an `Error` with a
//!   fallback value and completes;
//! - [`delay`](RxStreamExt::delay) — delays every event;
//! - [`timeout`](RxStreamExt::timeout) — emits `RpcError::Timeout` on silence;
//! - [`switch_map`](RxStreamExt::switch_map) — projects each value to an inner
//!   stream and emits from the latest one.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::future::FutureExt;
use ice_rpc::Event;

/// Extension trait adding reactive operators to any poll-based stream of
/// [`ice_rpc::Event`].
pub trait RxStreamExt<T, E>: futures_lite::Stream<Item = Event<T, E>> + Sized {
    /// Transforms every `Next` value with `f`. Terminal events are forwarded
    /// unchanged.
    fn map<U, F>(self, f: F) -> Map<Self, F, T, U, E>
    where
        F: FnMut(T) -> U,
    {
        Map::new(self, f)
    }

    /// Keeps only the `Next` values for which `f` returns `true`.
    fn filter<F>(self, f: F) -> Filter<Self, F, T, E>
    where
        F: FnMut(&T) -> bool,
    {
        Filter::new(self, f)
    }

    /// Emits at most `n` `Next` values, then forces a `Complete`.
    fn take(self, n: usize) -> Take<Self, T, E> {
        Take::new(self, n)
    }

    /// Ignores the first `n` `Next` values, then forwards the rest.
    fn skip(self, n: usize) -> Skip<Self, T, E> {
        Skip::new(self, n)
    }

    /// Emits only the first `Next` value, then forces a `Complete`.
    fn first(self) -> First<Self, fn(&T) -> bool, T, E> {
        self.first_with((|_| true) as fn(&T) -> bool)
    }

    /// Emits the first `Next` value matching `predicate`, then completes.
    fn first_with<F>(self, predicate: F) -> First<Self, F, T, E>
    where
        F: FnMut(&T) -> bool,
    {
        First::new(self, predicate)
    }

    /// Prefixes the stream with an initial `Next(value)`.
    fn start_with(self, value: T) -> StartWith<Self, T, E> {
        StartWith::new(self, value)
    }

    /// Transforms the error type `E` into `E2` with `f`.
    fn map_err<F, E2>(self, f: F) -> MapErr<Self, F, T, E, E2>
    where
        F: FnMut(E) -> E2,
    {
        MapErr::new(self, f)
    }

    /// Accumulates every `Next` value into a running state.
    fn scan<U, F>(self, initial: U, f: F) -> Scan<Self, F, T, U, E>
    where
        U: Clone,
        F: FnMut(U, T) -> U,
    {
        Scan::new(self, initial, f)
    }

    /// Runs a side effect on each `Next` value without altering it.
    fn tap<F>(self, f: F) -> Tap<Self, F, T, E>
    where
        F: FnMut(&T),
    {
        Tap::new(self, f)
    }

    /// Runs `f` exactly once when the stream terminates.
    fn finalize<F>(self, f: F) -> Finalize<Self, F, T, E>
    where
        F: FnOnce(),
    {
        Finalize::new(self, f)
    }

    /// Replaces an `Error` with a fallback value, then completes.
    fn catch_error<F>(self, f: F) -> CatchError<Self, F, T, E>
    where
        F: FnOnce(E) -> T,
    {
        CatchError::new(self, f)
    }

    /// Delays every event by `duration`.
    fn delay(self, duration: std::time::Duration) -> Delay<Self, T, E> {
        Delay::new(self, duration)
    }

    /// Emits `RpcError::Timeout` if no event arrives within `duration`.
    fn timeout(self, duration: std::time::Duration) -> Timeout<Self, T, E> {
        Timeout::new(self, duration)
    }

    /// Projects each value to an inner stream and emits from the latest one,
    /// cancelling previous subscriptions (RxJS `switchMap`).
    fn switch_map<F, U>(self, f: F) -> SwitchMap<Self, F, T, U, E>
    where
        F: FnMut(T) -> ice_rpc::Stream<U, E>,
    {
        SwitchMap::new(self, f)
    }

    /// Emits `RpcError::Cancelled` and stops once `token` is cancelled (RxJS
    /// `takeUntil`).
    fn take_until(self, token: &ice_rpc::CancellationToken) -> TakeUntil<Self, T, E> {
        TakeUntil::new(self, token.clone())
    }

    /// Awaits the first emitted value of the stream.
    ///
    /// Equivalent to `recv()` projected onto a `Result<T, StreamError<E>>`:
    /// `Next(v)` → `Ok(v)`, `Error(e)` → `Business(e)`, `RpcError(e)` → `Rpc(e)`,
    /// `Complete`/closed → `Empty`.
    #[allow(async_fn_in_trait)]
    async fn first_value(self) -> Result<T, ice_rpc::StreamError<E>>
    where
        Self: Sized,
    {
        let mut stream = Box::pin(self);
        loop {
            match futures_lite::future::poll_fn(|cx| {
                futures_lite::Stream::poll_next(stream.as_mut(), cx)
            })
            .await
            {
                Some(Event::Next(v)) => return Ok(v),
                Some(Event::Error(e)) => return Err(ice_rpc::StreamError::Business(e)),
                Some(Event::RpcError(e)) => return Err(ice_rpc::StreamError::Rpc(e)),
                Some(Event::Complete) | None => return Err(ice_rpc::StreamError::Empty),
            }
        }
    }

    /// Collects every emitted value into a `Vec`.
    ///
    /// The stream is consumed until `Complete` (or until it is closed). On
    /// `Error`/`RpcError` the collected values are discarded and the error is
    /// returned.
    #[allow(async_fn_in_trait)]
    async fn collect(self) -> Result<Vec<T>, ice_rpc::StreamError<E>>
    where
        Self: Sized,
    {
        let mut stream = Box::pin(self);
        let mut values = Vec::new();
        loop {
            match futures_lite::future::poll_fn(|cx| {
                futures_lite::Stream::poll_next(stream.as_mut(), cx)
            })
            .await
            {
                Some(Event::Next(v)) => values.push(v),
                Some(Event::Complete) => return Ok(values),
                Some(Event::Error(e)) => return Err(ice_rpc::StreamError::Business(e)),
                Some(Event::RpcError(e)) => return Err(ice_rpc::StreamError::Rpc(e)),
                None => return Ok(values),
            }
        }
    }
}

impl<S, T, E> RxStreamExt<T, E> for S where S: futures_lite::Stream<Item = Event<T, E>> + Sized {}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::map`].
    pub struct Map<S, F, T, U, E> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, U, E)>,
    }
}

impl<S, F, T, U, E> Map<S, F, T, U, E> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, U, E> futures_lite::Stream for Map<S, F, T, U, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(T) -> U,
{
    type Item = Event<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => Poll::Ready(Some(Event::Next((this.f)(v)))),
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
            Poll::Ready(Some(Event::Error(e))) => Poll::Ready(Some(Event::Error(e))),
            Poll::Ready(Some(Event::RpcError(e))) => Poll::Ready(Some(Event::RpcError(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::filter`].
    pub struct Filter<S, F, T, E> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> Filter<S, F, T, E> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for Filter<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    if (this.f)(&v) {
                        return Poll::Ready(Some(Event::Next(v)));
                    }
                }
                Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::take`].
    pub struct Take<S, T, E> {
        #[pin]
        stream: S,
        remaining: usize,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Take<S, T, E> {
    fn new(stream: S, n: usize) -> Self {
        Self {
            stream,
            remaining: n,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Take<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => {
                if *this.remaining == 0 {
                    *this.done = true;
                    return Poll::Ready(Some(Event::Complete));
                }
                *this.remaining -= 1;
                Poll::Ready(Some(Event::Next(v)))
            }
            Poll::Ready(Some(other)) => Poll::Ready(Some(other)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::skip`].
    pub struct Skip<S, T, E> {
        #[pin]
        stream: S,
        remaining: usize,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Skip<S, T, E> {
    fn new(stream: S, n: usize) -> Self {
        Self {
            stream,
            remaining: n,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Skip<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    if *this.remaining > 0 {
                        *this.remaining -= 1;
                    } else {
                        return Poll::Ready(Some(Event::Next(v)));
                    }
                }
                Poll::Ready(Some(other)) => return Poll::Ready(Some(other)),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::first_with`].
    pub struct First<S, F, T, E> {
        #[pin]
        stream: S,
        f: F,
        completed: bool,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> First<S, F, T, E> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            completed: false,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for First<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if *this.completed {
            *this.done = true;
            return Poll::Ready(Some(Event::Complete));
        }
        loop {
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    if (this.f)(&v) {
                        *this.completed = true;
                        return Poll::Ready(Some(Event::Next(v)));
                    }
                }
                Poll::Ready(Some(other)) => {
                    *this.done = true;
                    return Poll::Ready(Some(other));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::start_with`].
    pub struct StartWith<S, T, E> {
        #[pin]
        stream: S,
        first: Option<T>,
        _marker: PhantomData<E>,
    }
}

impl<S, T, E> StartWith<S, T, E> {
    fn new(stream: S, value: T) -> Self {
        Self {
            stream,
            first: Some(value),
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for StartWith<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(value) = this.first.take() {
            return Poll::Ready(Some(Event::Next(value)));
        }
        futures_lite::Stream::poll_next(this.stream.as_mut(), cx)
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::map_err`].
    pub struct MapErr<S, F, T, E, E2> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, E, E2)>,
    }
}

impl<S, F, T, E, E2> MapErr<S, F, T, E, E2> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E, E2> futures_lite::Stream for MapErr<S, F, T, E, E2>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(E) -> E2,
{
    type Item = Event<T, E2>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => Poll::Ready(Some(Event::Next(v))),
            Poll::Ready(Some(Event::Error(e))) => Poll::Ready(Some(Event::Error((this.f)(e)))),
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
            Poll::Ready(Some(Event::RpcError(e))) => Poll::Ready(Some(Event::RpcError(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::scan`].
    pub struct Scan<S, F, T, U, E> {
        #[pin]
        stream: S,
        f: F,
        acc: Option<U>,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, U, E> Scan<S, F, T, U, E> {
    fn new(stream: S, initial: U, f: F) -> Self {
        Self {
            stream,
            f,
            acc: Some(initial),
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, U, E> futures_lite::Stream for Scan<S, F, T, U, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    U: Clone,
    F: FnMut(U, T) -> U,
{
    type Item = Event<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => {
                let acc = this.acc.take().expect("scan accumulator");
                let next = (this.f)(acc, v);
                let out = next.clone();
                *this.acc = Some(next);
                Poll::Ready(Some(Event::Next(out)))
            }
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
            Poll::Ready(Some(Event::Error(e))) => Poll::Ready(Some(Event::Error(e))),
            Poll::Ready(Some(Event::RpcError(e))) => Poll::Ready(Some(Event::RpcError(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::tap`].
    pub struct Tap<S, F, T, E> {
        #[pin]
        stream: S,
        f: F,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> Tap<S, F, T, E> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for Tap<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(&T),
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => {
                (this.f)(&v);
                Poll::Ready(Some(Event::Next(v)))
            }
            Poll::Ready(Some(other)) => Poll::Ready(Some(other)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::finalize`].
    pub struct Finalize<S, F, T, E> {
        #[pin]
        stream: S,
        f: Option<F>,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> Finalize<S, F, T, E> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f: Some(f),
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for Finalize<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnOnce(),
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(event)) => {
                if event.is_terminal() {
                    if let Some(f) = this.f.take() {
                        f();
                    }
                }
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => {
                if let Some(f) = this.f.take() {
                    f();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::catch_error`].
    pub struct CatchError<S, F, T, E> {
        #[pin]
        stream: S,
        f: Option<F>,
        completed: bool,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, F, T, E> CatchError<S, F, T, E> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f: Some(f),
            completed: false,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, E> futures_lite::Stream for CatchError<S, F, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnOnce(E) -> T,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if *this.completed {
            *this.done = true;
            return Poll::Ready(Some(Event::Complete));
        }
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(Event::Next(v))) => Poll::Ready(Some(Event::Next(v))),
            Poll::Ready(Some(Event::Error(e))) => match this.f.take() {
                Some(f) => {
                    *this.completed = true;
                    Poll::Ready(Some(Event::Next(f(e))))
                }
                None => {
                    *this.done = true;
                    Poll::Ready(Some(Event::Complete))
                }
            },
            Poll::Ready(Some(other)) => {
                *this.done = true;
                Poll::Ready(Some(other))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::delay`].
    pub struct Delay<S, T, E> {
        #[pin]
        stream: S,
        duration: std::time::Duration,
        pending: Option<Event<T, E>>,
        sleep: Option<futures_lite::future::Boxed<()>>,
    }
}

impl<S, T, E> Delay<S, T, E> {
    fn new(stream: S, duration: std::time::Duration) -> Self {
        Self {
            stream,
            duration,
            pending: None,
            sleep: None,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Delay<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if this.pending.is_some() {
                let ready = this
                    .sleep
                    .as_mut()
                    .expect("sleep future present when an event is pending")
                    .as_mut()
                    .poll(cx);
                match ready {
                    Poll::Ready(()) => {
                        *this.sleep = None;
                        let event = this.pending.take().expect("pending event");
                        return Poll::Ready(Some(event));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(event)) => {
                    *this.pending = Some(event);
                    *this.sleep = Some(ice_rpc::rt::sleep(*this.duration).boxed());
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::timeout`].
    pub struct Timeout<S, T, E> {
        #[pin]
        stream: S,
        duration: std::time::Duration,
        sleep: Option<futures_lite::future::Boxed<()>>,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Timeout<S, T, E> {
    fn new(stream: S, duration: std::time::Duration) -> Self {
        Self {
            stream,
            duration,
            sleep: None,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for Timeout<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if this.sleep.is_none() {
            *this.sleep = Some(ice_rpc::rt::sleep(*this.duration).boxed());
        }
        match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
            Poll::Ready(Some(event)) => {
                // Reset the silence deadline after every received event.
                *this.sleep = Some(ice_rpc::rt::sleep(*this.duration).boxed());
                if event.is_terminal() {
                    *this.done = true;
                }
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => match this.sleep.as_mut().expect("sleep future").as_mut().poll(cx) {
                Poll::Ready(()) => {
                    *this.done = true;
                    Poll::Ready(Some(Event::RpcError(ice_rpc::RpcError::Timeout)))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::switch_map`].
    pub struct SwitchMap<S, F, T, U, E> {
        #[pin]
        stream: S,
        f: F,
        #[pin]
        inner: Option<ice_rpc::Stream<U, E>>,
        done: bool,
        _marker: PhantomData<T>,
    }
}

impl<S, F, T, U, E> SwitchMap<S, F, T, U, E> {
    fn new(stream: S, f: F) -> Self {
        Self {
            stream,
            f,
            inner: None,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, F, T, U, E> futures_lite::Stream for SwitchMap<S, F, T, U, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
    F: FnMut(T) -> ice_rpc::Stream<U, E>,
{
    type Item = Event<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if *this.done {
                return Poll::Ready(None);
            }
            if this.inner.is_some() {
                let result = {
                    let inner = this.inner.as_mut().as_pin_mut().expect("inner stream");
                    futures_lite::Stream::poll_next(inner, cx)
                };
                match result {
                    Poll::Ready(Some(Event::Next(u))) => return Poll::Ready(Some(Event::Next(u))),
                    Poll::Ready(Some(Event::Complete)) => {
                        this.inner.set(None);
                    }
                    Poll::Ready(Some(Event::Error(e))) => {
                        *this.done = true;
                        return Poll::Ready(Some(Event::Error(e)));
                    }
                    Poll::Ready(Some(Event::RpcError(e))) => {
                        *this.done = true;
                        return Poll::Ready(Some(Event::RpcError(e)));
                    }
                    Poll::Ready(None) => {
                        this.inner.set(None);
                    }
                    Poll::Pending => {}
                }
            }
            match futures_lite::Stream::poll_next(this.stream.as_mut(), cx) {
                Poll::Ready(Some(Event::Next(v))) => {
                    this.inner.set(Some((this.f)(v)));
                }
                Poll::Ready(Some(Event::Complete)) => {
                    *this.done = true;
                    return Poll::Ready(Some(Event::Complete));
                }
                Poll::Ready(Some(Event::Error(e))) => {
                    *this.done = true;
                    return Poll::Ready(Some(Event::Error(e)));
                }
                Poll::Ready(Some(Event::RpcError(e))) => {
                    *this.done = true;
                    return Poll::Ready(Some(Event::RpcError(e)));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// See [`RxStreamExt::take_until`].
    pub struct TakeUntil<S, T, E> {
        #[pin]
        stream: S,
        token: ice_rpc::CancellationToken,
        done: bool,
        _marker: PhantomData<(T, E)>,
    }
}

impl<S, T, E> TakeUntil<S, T, E> {
    fn new(stream: S, token: ice_rpc::CancellationToken) -> Self {
        Self {
            stream,
            token,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<S, T, E> futures_lite::Stream for TakeUntil<S, T, E>
where
    S: futures_lite::Stream<Item = Event<T, E>>,
{
    type Item = Event<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        if this.token.is_cancelled() {
            *this.done = true;
            return Poll::Ready(Some(Event::RpcError(ice_rpc::RpcError::Cancelled)));
        }
        futures_lite::Stream::poll_next(this.stream.as_mut(), cx)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::RxStreamExt;
    use crate::{from, of};
    use ice_rpc::Event;

    /// Drains a poll-based stream to completion.
    async fn drain<S, T, E>(stream: S) -> Vec<Event<T, E>>
    where
        S: futures_lite::Stream<Item = Event<T, E>>,
    {
        let mut stream = Box::pin(stream);
        let mut out = Vec::new();
        while let Some(event) =
            futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx))
                .await
        {
            out.push(event);
        }
        out
    }

    /// Reads the next event from a boxed, pinned stream.
    async fn next_event<S, T, E>(stream: &mut Pin<Box<S>>) -> Option<Event<T, E>>
    where
        S: futures_lite::Stream<Item = Event<T, E>>,
    {
        futures_lite::future::poll_fn(|cx| futures_lite::Stream::poll_next(stream.as_mut(), cx))
            .await
    }

    #[test]
    fn map_filter_take_pipeline() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(6);
        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_next(2)).unwrap();
        pollster::block_on(tx.send_next(3)).unwrap();
        pollster::block_on(tx.send_next(4)).unwrap();
        pollster::block_on(tx.send_next(5)).unwrap();
        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        let stream = rx.filter(|v| *v % 2 == 1).map(|v| v * 10).take(3);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], Event::Next(v) if *v == 10));
        assert!(matches!(&events[1], Event::Next(v) if *v == 30));
        assert!(matches!(&events[2], Event::Next(v) if *v == 50));
        assert!(matches!(&events[3], Event::Complete));
    }

    #[test]
    fn finalize_runs_on_complete() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let finalized = Arc::new(AtomicBool::new(false));
        let flag = finalized.clone();
        let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Complete));
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[test]
    fn finalize_runs_on_source_channel_close() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let finalized = Arc::new(AtomicBool::new(false));
        let flag = finalized.clone();
        let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

        pollster::block_on(tx.send_next(1)).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[test]
    fn finalize_runs_on_error() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let finalized = Arc::new(AtomicBool::new(false));
        let flag = finalized.clone();
        let stream = rx.finalize(move || flag.store(true, Ordering::SeqCst));

        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Error(e) if e.as_str() == "boom"));
        assert!(finalized.load(Ordering::SeqCst));
    }

    #[test]
    fn tap_runs_side_effect() {
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let seen = Arc::new(AtomicI32::new(0));
        let flag = seen.clone();
        let stream = rx.tap(move |_| {
            flag.fetch_add(1, Ordering::SeqCst);
        });

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_next(2)).unwrap();
        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        pollster::block_on(drain(stream));
        assert_eq!(seen.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn tap_does_not_touch_terminal_events() {
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let seen = Arc::new(AtomicI32::new(0));
        let flag = seen.clone();
        let stream = rx.tap(move |_| {
            flag.fetch_add(1, Ordering::SeqCst);
        });

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Error(e) if e.as_str() == "boom"));
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn delay_postpones_events() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.delay(std::time::Duration::from_millis(20));

        pollster::block_on(tx.send_next(1)).unwrap();
        drop(tx);

        let mut stream = Box::pin(stream);
        let start = std::time::Instant::now();
        let event = pollster::block_on(next_event(&mut stream));
        let elapsed = start.elapsed();

        assert!(matches!(event, Some(Event::Next(1))));
        assert!(elapsed >= std::time::Duration::from_millis(15));
    }

    #[test]
    fn delay_forwards_terminal_events() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.delay(std::time::Duration::from_millis(20));

        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        let mut stream = Box::pin(stream);
        let start = std::time::Instant::now();
        let event = pollster::block_on(next_event(&mut stream));
        let elapsed = start.elapsed();

        assert!(matches!(event, Some(Event::Complete)));
        assert!(elapsed >= std::time::Duration::from_millis(15));
    }

    #[test]
    fn catch_error_replaces_error_with_fallback() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.catch_error(|_| -1);

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == -1));
        assert!(matches!(&events[2], Event::Complete));
    }

    #[test]
    fn catch_error_forwards_rpc_error_unchanged() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let called = Arc::new(AtomicBool::new(false));
        let flag = called.clone();
        let stream = rx.catch_error(move |_| {
            flag.store(true, Ordering::SeqCst);
            -1
        });

        pollster::block_on(tx.send_event(Event::RpcError(ice_rpc::RpcError::Timeout))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::RpcError(_)));
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn catch_error_passthrough_when_no_error() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let called = Arc::new(AtomicBool::new(false));
        let flag = called.clone();
        let stream = rx.catch_error(move |_| {
            flag.store(true, Ordering::SeqCst);
            -1
        });

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Complete));
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn map_maps_normalized_single_value() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.map(|v| v * 2);

        pollster::block_on(tx.send_complete_with(5)).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 10));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn map_forwards_terminal_events_unchanged() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.map(|v| v * 2);

        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        pollster::block_on(tx.send_event(Event::RpcError(ice_rpc::RpcError::Timeout))).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Error(e) if e.as_str() == "boom"));
        assert!(matches!(&events[1], Event::RpcError(_)));
    }

    #[test]
    fn filter_normalizes_complete_with_as_value() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.filter(|v| *v % 2 == 1);

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_next(2)).unwrap();
        pollster::block_on(tx.send_complete_with(9)).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 9));
        assert!(matches!(&events[2], Event::Complete));
    }

    #[test]
    fn take_zero_completes_without_forwarding() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.take(0);

        pollster::block_on(tx.send_next(1)).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Complete));
    }

    #[test]
    fn take_forwards_source_terminal_before_limit() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.take(5);

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_next(2)).unwrap();
        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 2));
        assert!(matches!(&events[2], Event::Error(e) if e.as_str() == "boom"));
    }

    #[test]
    fn first_emits_only_first_value() {
        let stream = from([1, 2, 3]).first();

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn first_with_emits_first_matching_value() {
        let stream = from([1, 2, 3]).first_with(|v| *v >= 2);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 2));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn first_forwards_error_before_any_value() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.first();

        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Error(e) if e.as_str() == "boom"));
    }

    #[test]
    fn first_completes_empty_when_no_value() {
        let stream = from(std::iter::empty::<i32>()).first();

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Complete));
    }

    #[test]
    fn first_with_completes_empty_when_no_match() {
        let stream = from([1, 2, 3]).first_with(|v| *v > 10);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Complete));
    }

    #[test]
    fn first_with_matches_single_of_value() {
        let stream = of(7).first_with(|v| *v > 5);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 7));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn map_err_transforms_error_type() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.map_err(|e| e.len());

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_error("boom".to_string())).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Error(n) if *n == 4));
    }

    #[test]
    fn scan_emits_running_accumulator() {
        let stream = from([1, 2, 3]).scan(0, |acc, v| acc + v);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 3));
        assert!(matches!(&events[2], Event::Next(v) if *v == 6));
        assert!(matches!(&events[3], Event::Complete));
    }

    #[test]
    fn start_with_prefixes_initial_value() {
        let stream = of(1).start_with(0);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], Event::Next(v) if *v == 0));
        assert!(matches!(&events[1], Event::Next(v) if *v == 1));
        assert!(matches!(&events[2], Event::Complete));
    }

    #[test]
    fn skip_drops_leading_values() {
        let stream = from([1, 2, 3, 4]).skip(2);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], Event::Next(v) if *v == 3));
        assert!(matches!(&events[1], Event::Next(v) if *v == 4));
        assert!(matches!(&events[2], Event::Complete));
    }

    #[test]
    fn timeout_emits_rpc_error_on_silence() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.timeout(std::time::Duration::from_millis(20));

        let mut stream = Box::pin(stream);
        let event = pollster::block_on(next_event(&mut stream));
        assert!(matches!(event, Some(Event::RpcError(_))));

        drop(tx);
    }

    #[test]
    fn timeout_forwards_values_before_deadline() {
        let (tx, rx) = ice_rpc::channel::<i32, String>(crate::OPERATOR_CHANNEL_CAPACITY);
        let stream = rx.timeout(std::time::Duration::from_millis(200));

        pollster::block_on(tx.send_next(1)).unwrap();
        pollster::block_on(tx.send_complete()).unwrap();
        drop(tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn switch_map_switches_to_latest_inner_and_cancels_previous() {
        use std::sync::{Arc, Mutex};

        let (outer_tx, outer_rx) = ice_rpc::channel::<i32, String>(8);
        let senders: Arc<Mutex<Vec<ice_rpc::Sender<i32, String>>>> =
            Arc::new(Mutex::new(Vec::new()));

        let senders_for_task = senders.clone();
        let stream = outer_rx.switch_map(move |_| {
            let (tx, rx) = ice_rpc::channel::<i32, String>(8);
            senders_for_task.lock().unwrap().push(tx);
            rx
        });

        pollster::block_on(outer_tx.send_next(1)).unwrap();
        pollster::block_on(outer_tx.send_next(2)).unwrap();

        let mut stream = Box::pin(stream);
        // A single poll drives the lazy combinator: it consumes both source
        // values and subscribes twice.
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(&waker);
        let _ = futures_lite::Stream::poll_next(stream.as_mut(), &mut cx);
        assert_eq!(senders.lock().unwrap().len(), 2);

        let inner2_tx = senders.lock().unwrap()[1].clone();
        pollster::block_on(inner2_tx.send_next(20)).unwrap();

        assert!(matches!(
            pollster::block_on(next_event(&mut stream)),
            Some(Event::Next(v)) if v == 20
        ));

        let inner1_tx = senders.lock().unwrap()[0].clone();
        assert!(pollster::block_on(inner1_tx.send_next(10)).is_err());

        pollster::block_on(outer_tx.send_complete()).unwrap();
        drop(outer_tx);

        assert!(matches!(
            pollster::block_on(next_event(&mut stream)),
            Some(Event::Complete)
        ));
    }

    #[test]
    fn switch_map_forwards_inner_error() {
        let (outer_tx, outer_rx) = ice_rpc::channel::<i32, String>(8);
        let (inner_tx, inner_rx) = ice_rpc::channel::<i32, String>(8);

        let stream = outer_rx.switch_map(move |_| inner_rx.clone());

        pollster::block_on(outer_tx.send_next(1)).unwrap();
        pollster::block_on(inner_tx.send_error("boom".to_string())).unwrap();
        drop(outer_tx);
        drop(inner_tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::Error(e) if e.as_str() == "boom"));
    }

    #[test]
    fn switch_map_ignores_inner_complete() {
        let (outer_tx, outer_rx) = ice_rpc::channel::<i32, String>(8);
        let (inner1_tx, inner1_rx) = ice_rpc::channel::<i32, String>(8);
        let (inner2_tx, inner2_rx) = ice_rpc::channel::<i32, String>(8);

        let stream = outer_rx.switch_map(move |v| {
            if v == 1 {
                inner1_rx.clone()
            } else {
                inner2_rx.clone()
            }
        });

        pollster::block_on(outer_tx.send_next(1)).unwrap();
        pollster::block_on(inner1_tx.send_complete()).unwrap();
        pollster::block_on(outer_tx.send_next(2)).unwrap();
        pollster::block_on(inner2_tx.send_next(20)).unwrap();
        pollster::block_on(outer_tx.send_complete()).unwrap();
        drop(outer_tx);
        drop(inner1_tx);
        drop(inner2_tx);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Event::Next(v) if *v == 20));
        assert!(matches!(&events[1], Event::Complete));
    }

    #[test]
    fn take_until_emits_cancelled_when_token_fires() {
        let token = ice_rpc::CancellationToken::new();
        token.cancel();
        let stream = from([1, 2, 3]).take_until(&token);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Event::RpcError(_)));
    }

    #[test]
    fn take_until_forwards_values_when_not_cancelled() {
        let token = ice_rpc::CancellationToken::new();
        let stream = from([1, 2, 3]).take_until(&token);

        let events = pollster::block_on(drain(stream));
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], Event::Next(v) if *v == 1));
        assert!(matches!(&events[1], Event::Next(v) if *v == 2));
        assert!(matches!(&events[2], Event::Next(v) if *v == 3));
        assert!(matches!(&events[3], Event::Complete));
    }
}
