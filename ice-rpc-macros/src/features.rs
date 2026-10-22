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
///
/// Two switches, and no more: a service does not know which process will call it,
/// so there is nothing finer to express here. The JSON block covers both
/// directions, and the link-time reachability of `Proxy::provide_json()` — not a
/// feature — decides whether a given binary carries the host half.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Features {
    /// The `{Trait}Decoder` and the generated `Display` implementation used by an
    /// out-of-band observer.
    pub monitoring: bool,
    /// The whole JSON view: the `impl JsonInvoker` a JSON transport dispatches to,
    /// the rkyv ↔ `serde_json::Value` converters, and the `ProviderJson` mode.
    pub json: bool,
}

impl Features {
    /// The set of the build this macro is compiled in.
    ///
    /// The only place in the crate that reads a Cargo feature.
    pub const fn from_cfg() -> Self {
        Self {
            monitoring: cfg!(feature = "monitoring"),
            json: cfg!(feature = "json"),
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
        if self.json {
            names.push("json");
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
            json: false,
        };
        let json_only = Features {
            monitoring: false,
            json: true,
        };
        let all = Features {
            monitoring: true,
            json: true,
        };

        assert_eq!(none.to_string(), "none");
        assert_eq!(json_only.to_string(), "json");
        assert_eq!(all.to_string(), "monitoring.json");
    }

    /// The only link between the Cargo features and the expansion: if this one
    /// breaks, every feature silently stops gating anything.
    #[test]
    fn from_cfg_matches_the_features_the_build_enabled() {
        let features = Features::from_cfg();
        assert_eq!(features.monitoring, cfg!(feature = "monitoring"));
        assert_eq!(features.json, cfg!(feature = "json"));
    }
}
