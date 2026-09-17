//! Which optional blocks of the expansion a build asks for.
//!
//! The generators used to read `cfg!(feature = ...)` themselves. That had two
//! costs:
//!
//! - the expansion depended on the feature set of whatever build compiled the
//!   crate, which is not a property of the service being expanded, and is
//!   unified across a whole workspace build — so the same `#[service]` produced
//!   different code depending on which *other* crate asked for which feature;
//! - the golden test could only check the configuration its own build happened
//!   to have, one file per combination, and never a combination that build did
//!   not enable.
//!
//! Passing the set as a value keeps the decision in one place — [`Features::from_cfg`]
//! for the real macro, an explicit set for a test — and lets the golden test
//! expand the same trait in every configuration it pins, in every build.

use std::fmt;

/// The optional blocks of one expansion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Features {
    /// The `{Trait}Decoder` and the generated `Display` implementation used by an
    /// out-of-band observer.
    pub monitoring: bool,
    /// The rkyv ↔ `serde_json::Value` converters and the `ProviderNodeJs` mode
    /// used by the Node.js gateway.
    pub nodejs: bool,
    /// The `HttpCallable` implementation used by the HTTP gateway.
    pub http: bool,
}

impl Features {
    /// The set of the build this macro is compiled in.
    ///
    /// The only place in the crate that reads a Cargo feature.
    pub const fn from_cfg() -> Self {
        Self {
            monitoring: cfg!(feature = "monitoring"),
            nodejs: cfg!(feature = "nodejs"),
            http: cfg!(feature = "http"),
        }
    }
}

impl fmt::Display for Features {
    /// Names the set as a stable, dot-separated list, for a file name or a log.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names = Vec::new();
        if self.monitoring {
            names.push("monitoring");
        }
        if self.nodejs {
            names.push("nodejs");
        }
        if self.http {
            names.push("http");
        }
        if names.is_empty() {
            return f.write_str("none");
        }
        f.write_str(&names.join("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_name_of_a_set_lists_its_blocks_in_a_stable_order() {
        let none = Features {
            monitoring: false,
            nodejs: false,
            http: false,
        };
        let all = Features {
            monitoring: true,
            nodejs: true,
            http: true,
        };
        let nodejs_only = Features {
            monitoring: false,
            nodejs: true,
            http: false,
        };

        assert_eq!(none.to_string(), "none");
        assert_eq!(all.to_string(), "monitoring.nodejs.http");
        assert_eq!(nodejs_only.to_string(), "nodejs");
    }

    /// The only link between the Cargo features and the expansion: if this one
    /// breaks, every feature silently stops gating anything.
    #[test]
    fn from_cfg_matches_the_features_the_build_enabled() {
        let features = Features::from_cfg();
        assert_eq!(features.monitoring, cfg!(feature = "monitoring"));
        assert_eq!(features.nodejs, cfg!(feature = "nodejs"));
        assert_eq!(features.http, cfg!(feature = "http"));
    }
}
