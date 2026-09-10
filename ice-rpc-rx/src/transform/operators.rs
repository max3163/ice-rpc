//! One pull-based combinator per operator.
//!
//! Every type here wraps a source stream and implements `futures_lite::Stream`;
//! the trait methods live in `super`.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::future::FutureExt;
use ice_rpc::Event;

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
    pub(super) fn new(stream: S, f: F) -> Self {
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
    pub(super) fn new(stream: S, f: F) -> Self {
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
    pub(super) fn new(stream: S, n: usize) -> Self {
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
    pub(super) fn new(stream: S, n: usize) -> Self {
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
    pub(super) fn new(stream: S, f: F) -> Self {
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
    pub(super) fn new(stream: S, value: T) -> Self {
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
    pub(super) fn new(stream: S, f: F) -> Self {
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
            // Only the business error is remapped; technical errors pass through.
            Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Business(e)))) => {
                Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Business(
                    (this.f)(e),
                ))))
            }
            Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Technical(e)))) => {
                Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Technical(e))))
            }
            Poll::Ready(Some(Event::Complete)) => Poll::Ready(Some(Event::Complete)),
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
    pub(super) fn new(stream: S, initial: U, f: F) -> Self {
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
    pub(super) fn new(stream: S, f: F) -> Self {
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
    pub(super) fn new(stream: S, f: F) -> Self {
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
    pub(super) fn new(stream: S, f: F) -> Self {
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
            // Only a business error can be caught; a technical error is fatal.
            Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Business(e)))) => {
                match this.f.take() {
                    Some(f) => {
                        *this.completed = true;
                        Poll::Ready(Some(Event::Next(f(e))))
                    }
                    None => {
                        *this.done = true;
                        Poll::Ready(Some(Event::Complete))
                    }
                }
            }
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
    pub(super) fn new(stream: S, duration: std::time::Duration) -> Self {
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
    pub(super) fn new(stream: S, duration: std::time::Duration) -> Self {
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
                    Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Technical(
                        ice_rpc::RpcError::Timeout,
                    ))))
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
        inner: Option<ice_rpc::Observable<U, E>>,
        done: bool,
        _marker: PhantomData<T>,
    }
}

impl<S, F, T, U, E> SwitchMap<S, F, T, U, E> {
    pub(super) fn new(stream: S, f: F) -> Self {
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
    F: FnMut(T) -> ice_rpc::Observable<U, E>,
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
    pub(super) fn new(stream: S, token: ice_rpc::CancellationToken) -> Self {
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
            return Poll::Ready(Some(Event::Error(ice_rpc::ObservableError::Technical(
                ice_rpc::RpcError::Cancelled,
            ))));
        }
        futures_lite::Stream::poll_next(this.stream.as_mut(), cx)
    }
}
