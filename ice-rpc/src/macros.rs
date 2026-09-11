//! Utility macros for ice-rpc.
//!
//! Internal only: these macros are not part of the public API.

/// `try_or_log!` macro: evaluates an expression and, on error, logs
/// a message and returns the specified value.
///
/// Internal helper — consumed through `crate::macros::try_or_log`.
///
/// # Variants
/// - `try_or_log!(expr, ctx, msg, ret)` — with a context prefix, returns `ret`.
/// - `try_or_log!(expr, ctx, msg)` — with a context prefix, returns `()`.
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

pub(crate) use try_or_log;
