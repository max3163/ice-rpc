//! Macros and utilities for consuming RPC streams.
//!
//! Provides `take_one` and `take_one_or_cancel` to extract the first
//! value of an `Observable`, as well as the `try_or_log!` macro for
//! error handling with logging.

use futures::FutureExt;

/// `try_or_log!` macro: evaluates an expression and, on error, logs
/// a message and returns the specified value.
///
/// # Variants
/// - `try_or_log!(expr, ctx, msg, ret)` — with a context prefix, returns `ret`.
/// - `try_or_log!(expr, ctx, msg)` — with a context prefix, returns `()`.
#[macro_export]
macro_rules! try_or_log {
    ($expr:expr, $ctx:expr, $msg:literal, $ret:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}: {} : {:?}", $ctx, $msg, e);
                return $ret;
            }
        }
    };
    ($expr:expr, $ctx:expr, $msg:literal) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}: {} : {:?}", $ctx, $msg, e);
                return;
            }
        }
    };
}

/// Consumes an `Observable<T, E>` and returns the first received value.
///
/// # Returns
/// * `Ok(v)` if `Event::Next(v)` is received.
/// * `Err(TakeOneError::Service(e))` if `Event::Error(e)`.
/// * `Err(TakeOneError::Empty)` if `Event::Complete` or the channel is closed.
/// * `Err(TakeOneError::Ipc(e))` on initial IPC error.
pub async fn take_one<T, E: std::fmt::Display>(
    observable: crate::Observable<T, E>,
) -> Result<T, crate::TakeOneError<E>> {
    match observable {
        Err(rpc_err) => Err(crate::TakeOneError::Ipc(rpc_err)),
        Ok(stream) => match stream.recv().await {
            Ok(crate::Event::Next(value)) => Ok(value),
            Ok(crate::Event::Error(e)) => Err(crate::TakeOneError::Service(e)),
            Ok(crate::Event::RpcError(e)) => Err(crate::TakeOneError::Ipc(e)),
            Ok(crate::Event::Complete) | Err(_) => Err(crate::TakeOneError::Empty),
        },
    }
}

/// Compatibility macro delegating to `take_one`.
#[macro_export]
macro_rules! take_one {
    ($observable:expr) => {
        $crate::take_one($observable).await
    };
}

/// Cancellable variant of `take_one`.
///
/// Races the reception against the local and global cancellation tokens.
///
/// # Returns
/// * `None` if cancelled before the response.
/// * `Some(Ok(v))` if a value is received.
/// * `Some(Err(...))` on error.
pub async fn take_one_or_cancel<T, E: std::fmt::Display>(
    observable: crate::Observable<T, E>,
    cancel: &crate::rt::CancellationToken,
) -> Option<Result<T, crate::TakeOneError<E>>> {
    match observable {
        Err(rpc_err) => Some(Err(crate::TakeOneError::Ipc(rpc_err))),
        Ok(stream) => {
            futures::select! {
                _ = cancel.cancelled().fuse() => None,
                _ = crate::global_cancel_token().cancelled().fuse() => None,
                event = stream.recv().fuse() => Some(match event {
                    Ok(crate::Event::Next(v))  => Ok(v),
                    Ok(crate::Event::Error(e)) => Err(crate::TakeOneError::Service(e)),
                    Ok(crate::Event::RpcError(e)) => Err(crate::TakeOneError::Ipc(e)),
                    Ok(crate::Event::Complete) | Err(_) => Err(crate::TakeOneError::Empty),
                }),
            }
        }
    }
}

/// Compatibility macro delegating to [`take_one_or_cancel`].
#[macro_export]
macro_rules! take_one_or_cancel {
    ($observable:expr, $cancel:expr) => {
        $crate::take_one_or_cancel($observable, $cancel).await
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;

    #[test]
    fn take_one_next_value() {
        let (tx, rx) = crate::channel::<i32, String>(1);
        pollster::block_on(tx.send(Event::Next(42))).unwrap();
        drop(tx);

        let result: Result<i32, crate::TakeOneError<String>> = pollster::block_on(take_one(Ok(rx)));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn take_one_error_event() {
        let (tx, rx) = crate::channel::<i32, String>(1);
        pollster::block_on(tx.send(Event::Error("oops".to_string()))).unwrap();
        drop(tx);

        let result: Result<i32, crate::TakeOneError<String>> = pollster::block_on(take_one(Ok(rx)));
        match result {
            Err(crate::TakeOneError::Service(e)) => assert_eq!(e, "oops"),
            other => panic!("Expected TakeOneError::Service, got {:?}", other),
        }
    }

    #[test]
    fn take_one_complete_is_empty() {
        let (tx, rx) = crate::channel::<i32, String>(1);
        pollster::block_on(tx.send(Event::Complete)).unwrap();
        drop(tx);

        let result: Result<i32, crate::TakeOneError<String>> = pollster::block_on(take_one(Ok(rx)));
        match result {
            Err(crate::TakeOneError::Empty) => {}
            other => panic!("Expected TakeOneError::Empty, got {:?}", other),
        }
    }

    #[test]
    fn take_one_closed_channel_is_empty() {
        let (_tx, rx) = crate::channel::<i32, String>(1);
        drop(_tx);

        let result: Result<i32, crate::TakeOneError<String>> = pollster::block_on(take_one(Ok(rx)));
        match result {
            Err(crate::TakeOneError::Empty) => {}
            other => panic!("Expected TakeOneError::Empty, got {:?}", other),
        }
    }

    #[test]
    fn take_one_ipc_error() {
        let result: Result<i32, crate::TakeOneError<String>> =
            pollster::block_on(take_one(Err(crate::RpcError::Timeout)));
        match result {
            Err(crate::TakeOneError::Ipc(crate::RpcError::Timeout)) => {}
            other => panic!("Expected TakeOneError::Ipc(Timeout), got {:?}", other),
        }
    }

    #[test]
    fn take_one_rpc_error_event() {
        let (tx, rx) = crate::channel::<i32, String>(1);
        pollster::block_on(tx.send(Event::RpcError(crate::RpcError::Timeout))).unwrap();
        drop(tx);

        let result: Result<i32, crate::TakeOneError<String>> = pollster::block_on(take_one(Ok(rx)));
        match result {
            Err(crate::TakeOneError::Ipc(crate::RpcError::Timeout)) => {}
            other => panic!("Expected TakeOneError::Ipc(Timeout), got {:?}", other),
        }
    }

    #[test]
    fn take_one_or_cancel_next_value() {
        let cancel = crate::rt::CancellationToken::new();
        let (tx, rx) = crate::channel::<i32, String>(1);
        pollster::block_on(tx.send(Event::Next(99))).unwrap();
        drop(tx);

        let result = pollster::block_on(take_one_or_cancel(Ok(rx), &cancel));
        assert!(matches!(result, Some(Ok(99))));
    }

    #[test]
    fn take_one_or_cancel_cancelled_before_value() {
        let cancel = crate::rt::CancellationToken::new();
        cancel.cancel();
        let (_tx, rx) = crate::channel::<i32, String>(1);

        let result = pollster::block_on(take_one_or_cancel(Ok(rx), &cancel));
        assert!(result.is_none());
    }

    #[test]
    fn take_one_or_cancel_ipc_error() {
        let cancel = crate::rt::CancellationToken::new();
        let result = pollster::block_on(take_one_or_cancel::<i32, String>(
            Err(crate::RpcError::IpcError("boom".into())),
            &cancel,
        ));
        match result {
            Some(Err(crate::TakeOneError::Ipc(_))) => {}
            other => panic!("Expected Some(Err(TakeOneError::Ipc(_))), got {:?}", other),
        }
    }

    #[test]
    fn take_one_or_cancel_rpc_error_event() {
        let cancel = crate::rt::CancellationToken::new();
        let (tx, rx) = crate::channel::<i32, String>(1);
        pollster::block_on(tx.send(Event::RpcError(crate::RpcError::Timeout))).unwrap();
        drop(tx);

        let result = pollster::block_on(take_one_or_cancel(Ok(rx), &cancel));
        match result {
            Some(Err(crate::TakeOneError::Ipc(crate::RpcError::Timeout))) => {}
            other => panic!(
                "Expected Some(Err(TakeOneError::Ipc(Timeout))), got {:?}",
                other
            ),
        }
    }

    #[test]
    fn take_one_or_cancel_error_event() {
        let cancel = crate::rt::CancellationToken::new();
        let (tx, rx) = crate::channel::<i32, String>(1);
        pollster::block_on(tx.send(Event::Error("fail".to_string()))).unwrap();
        drop(tx);

        let result = pollster::block_on(take_one_or_cancel(Ok(rx), &cancel));
        match result {
            Some(Err(crate::TakeOneError::Service(e))) => assert_eq!(e, "fail"),
            other => panic!(
                "Expected Some(Err(TakeOneError::Service(_))), got {:?}",
                other
            ),
        }
    }
}
