//! Stream creation helpers.
//!
//! [`from`] and [`of`] build local [`ice_rpc::Stream`] values, mirroring the
//! RxJS constructors of the same name.

use crate::RxError;

/// Creates a [`ice_rpc::Stream`] from an iterator, emitting each value as `Next` then
/// `Complete`.
///
/// Equivalent to RxJS `from`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::from;
///
/// let stream = from([1, 2, 3]);
/// ```
pub fn from<T, I>(iter: I) -> ice_rpc::Stream<T, RxError>
where
    I: IntoIterator<Item = T>,
    T: Send + 'static,
{
    // Collect upfront so the iterator itself does not need to be `Send`: only
    // the resulting `Vec<T>` is moved into the spawned task.
    let values: Vec<T> = iter.into_iter().collect();
    let (tx, rx) = ice_rpc::channel::<T, RxError>(crate::OPERATOR_CHANNEL_CAPACITY);
    ice_rpc::rt::spawn(async move {
        for value in values {
            if tx.send_next(value).await.is_err() {
                return;
            }
        }
        let _ = tx.send_complete().await;
    });
    rx
}

/// Creates a single-value [`ice_rpc::Stream`].
///
/// Consumers observe the value as `Next` followed by `Complete`: the transport
/// optimization `CompleteWith` is used internally and normalized away.
/// Equivalent to RxJS `of`.
///
/// # Example
/// ```rust,ignore
/// use ice_rpc_rx::of;
///
/// let stream = of(42);
/// ```
pub fn of<T>(value: T) -> ice_rpc::Stream<T, RxError>
where
    T: Send + 'static,
{
    // `of` emits the value as a transport-level `CompleteWith`, then the
    // returned stream is normalized so consumers observe `Next` + `Complete`.
    let (tx, rx) = ice_rpc::channel::<T, RxError>(1);
    ice_rpc::rt::spawn(async move {
        let _ = tx.send_complete_with(value).await;
    });
    // `Stream::recv` already normalizes `CompleteWith` into `Next` + `Complete`.
    rx
}
