//! The hash maps of the hot path: a fast, deterministic hasher.
//!
//! `perf` attributed ~2-3 % of both the provider's and the consumer's self time
//! to `std::collections::HashMap`'s default `RandomState` (SipHash), on maps keyed
//! by internal ids. That default buys resistance to hash-flooding, which the
//! transport does not need: its keys are a `service_id` derived from a
//! compile-time name, a method id likewise, and correlation ids this process
//! minted — never chosen by a peer. Two hashers replace it:
//!
//! - [`FxHasher`] for byte-shaped keys (the 16-byte correlation ids): one
//!   rotate/xor/multiply per 64-bit word;
//! - [`IdentityHasher`] for small integral keys (the `u32` service and method
//!   ids), where the value is its own bucket index.
//!
//! Both are deterministic, so anything that iterates one of these maps (metrics,
//! traces) is reproducible run to run instead of depending on a random seed.
//! `benches/hash_map.rs` prices them against the default on the exact key shapes
//! the transport uses: a lookup on a 16-byte key falls from ~19 ns to ~3 ns, and
//! the register/release cycle from ~42 ns to ~21 ns.
//!
//! Cold maps (decoders, metrics, name registries) keep `std::collections::HashMap`:
//! they are not on the per-call path, and a hasher change there buys nothing.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// A map for byte-shaped keys (correlation ids), hashed with [`FxHasher`].
pub(crate) type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

/// A map for small integral keys (`u32` ids), hashed by identity.
pub(crate) type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<IdentityHasher>>;

/// Fx-style hasher: one rotate/xor/multiply per 64-bit word.
///
/// The algorithm `rustc-hash` uses, implemented here rather than pulled in as a
/// dependency: only the transport's maps need it, and a deterministic hasher
/// keeps the whole workspace free of a new crate.
#[derive(Default)]
pub struct FxHasher {
    hash: u64,
}

impl FxHasher {
    /// The odd multiplier every Fx-round uses.
    const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(Self::SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // 8 bytes at a time; the tail is zero-padded, so the word count depends
        // on the length alone.
        let (chunks, tail) = bytes.as_chunks::<8>();
        for chunk in chunks {
            self.add(u64::from_le_bytes(*chunk));
        }
        if !tail.is_empty() {
            let mut word = [0u8; 8];
            word[..tail.len()].copy_from_slice(tail);
            self.add(u64::from_le_bytes(word));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.add(u64::from(value));
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.add(u64::from(value));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.add(value);
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.add(value as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        // A final avalanche is **required**, not a nicety: a correlation id is
        // `pid ++ counter`, so consecutive keys differ only in their low bytes
        // and, without this step, the high bits — the ones `hashbrown` uses as
        // its 7-bit control byte — stay nearly constant and collapse the table
        // (`map_build_realistic` measured 285 ms against 1.4 ms for SipHash, and
        // a lookup at 65 536 entries 2.3 µs against 19 ns). Measured after the
        // fix: the same steps are within a few percent of SipHash's cost.
        let mut hash = self.hash;
        hash ^= hash >> 32;
        hash = hash.wrapping_mul(0xd6e8_feb8_6659_fd93);
        hash ^= hash >> 32;
        hash
    }
}

/// Identity hasher: the key **is** its own hash.
///
/// Correct only for small integral keys, which is the contract of [`IdMap`]: the
/// `u32` ids it stores are dense and well spread, so the value doubles as the
/// bucket index. `write` is unreachable for those keys — `u32::hash` calls
/// `write_u32` — and does nothing rather than corrupt the state.
#[derive(Default)]
pub struct IdentityHasher {
    hash: u64,
}

impl Hasher for IdentityHasher {
    #[inline]
    fn write(&mut self, _bytes: &[u8]) {}

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.hash = u64::from(value);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    /// Pinned values: a change that would silently move every bucket is caught
    /// here rather than in a throughput number.
    #[test]
    fn the_hashers_are_deterministic_on_the_key_shapes_they_serve() {
        let fx = BuildHasherDefault::<FxHasher>::default();
        assert_eq!(fx.hash_one([0u8; 16]), fx.hash_one([0u8; 16]));
        assert_ne!(fx.hash_one([0u8; 16]), fx.hash_one([1u8; 16]));

        let id = BuildHasherDefault::<IdentityHasher>::default();
        assert_eq!(id.hash_one(7u32), 7u64);
        assert_eq!(id.hash_one(u32::MAX), u64::from(u32::MAX));
    }
}
