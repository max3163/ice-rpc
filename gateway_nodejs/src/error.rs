//! Failure model of the Node.js gateway.
//!
//! Every failure the gateway reports to JavaScript carries a **stable code**
//! from [`GatewayErrorCode`]. `napi::Error` exposes no custom `code` property,
//! so the code is rendered as the message prefix (`"E_TIMEOUT: …"`) and that
//! prefix is the contract: it is what JavaScript switches on and what the tests
//! assert. The codes are listed in
//! `docs/nodejs-gateway-api-v2.md` and must stay in sync with it.

/// Stable failure codes exposed to JavaScript.
///
/// Variant names are Rust-side; [`GatewayErrorCode::as_str`] is the wire value
/// and is the only thing JavaScript should depend on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayErrorCode {
    /// The call is illegal for the current lifecycle phase.
    GatewayState,
    /// The service name is absent from the Node.js provider inventory.
    UnknownService,
    /// The service exists but the method is not part of its surface.
    UnknownMethod,
    /// The arguments could not be decoded into the declared parameter types.
    InvalidArgs,
    /// No peer is connected for the requested service.
    NoProvider,
    /// The transport failed for a reason that is not a missing peer.
    Transport,
    /// The peer did not answer within the bridge deadline.
    Timeout,
    /// The service returned a business error, carried by the payload.
    Business,
    /// The JS dispatcher could not be reached (callback dropped or refusing).
    Callback,
    /// Too many calls are already waiting for a JS answer.
    PendingLimit,
    /// A call with the same correlation id is already in flight.
    DuplicateCid,
    /// The correlation id is not a hexadecimal id.
    InvalidCid,
    /// No pending call matches this correlation id (expired or resolved twice).
    UnknownCid,
}

impl GatewayErrorCode {
    /// The value JavaScript receives as the message prefix.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GatewayState => "E_GATEWAY_STATE",
            Self::UnknownService => "E_UNKNOWN_SERVICE",
            Self::UnknownMethod => "E_UNKNOWN_METHOD",
            Self::InvalidArgs => "E_INVALID_ARGS",
            Self::NoProvider => "E_NO_PROVIDER",
            Self::Transport => "E_TRANSPORT",
            Self::Timeout => "E_TIMEOUT",
            Self::Business => "E_BUSINESS",
            Self::Callback => "E_CALLBACK",
            Self::PendingLimit => "E_PENDING_LIMIT",
            Self::DuplicateCid => "E_DUPLICATE_CID",
            Self::InvalidCid => "E_INVALID_CID",
            Self::UnknownCid => "E_UNKNOWN_CID",
        }
    }
}

/// A gateway failure ready to be thrown at JavaScript.
#[derive(Debug, Clone)]
pub struct GatewayError {
    code: GatewayErrorCode,
    message: String,
}

impl GatewayError {
    /// Builds an error from its code and a human-readable message.
    pub fn new(code: GatewayErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// The stable code of this failure.
    pub const fn code(&self) -> GatewayErrorCode {
        self.code
    }

    /// The human-readable part, without the code prefix.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Renders the error exactly as JavaScript sees it: `"<CODE>: <message>"`.
    pub fn rendered(&self) -> String {
        format!("{}: {}", self.code.as_str(), self.message)
    }
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.rendered())
    }
}

impl std::error::Error for GatewayError {}

impl From<GatewayError> for napi::Error {
    fn from(error: GatewayError) -> Self {
        napi::Error::new(napi::Status::GenericFailure, error.rendered())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_error_starts_with_its_code() {
        let error = GatewayError::new(GatewayErrorCode::UnknownService, "unknown service 'Foo'");
        assert_eq!(error.rendered(), "E_UNKNOWN_SERVICE: unknown service 'Foo'");
        assert_eq!(error.code(), GatewayErrorCode::UnknownService);
        assert_eq!(error.message(), "unknown service 'Foo'");
        assert_eq!(error.to_string(), error.rendered());
    }

    #[test]
    fn every_code_renders_a_distinct_prefix() {
        const CODES: [GatewayErrorCode; 13] = [
            GatewayErrorCode::GatewayState,
            GatewayErrorCode::UnknownService,
            GatewayErrorCode::UnknownMethod,
            GatewayErrorCode::InvalidArgs,
            GatewayErrorCode::NoProvider,
            GatewayErrorCode::Transport,
            GatewayErrorCode::Timeout,
            GatewayErrorCode::Business,
            GatewayErrorCode::Callback,
            GatewayErrorCode::PendingLimit,
            GatewayErrorCode::DuplicateCid,
            GatewayErrorCode::InvalidCid,
            GatewayErrorCode::UnknownCid,
        ];
        let mut prefixes: Vec<&str> = CODES.iter().map(|code| code.as_str()).collect();
        let total = prefixes.len();
        prefixes.sort_unstable();
        prefixes.dedup();
        assert_eq!(prefixes.len(), total, "two codes share the same prefix");
        assert!(prefixes.iter().all(|prefix| prefix.starts_with("E_")));
    }
}
