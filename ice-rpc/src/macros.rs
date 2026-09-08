//! Utility macros for ice-rpc.

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
