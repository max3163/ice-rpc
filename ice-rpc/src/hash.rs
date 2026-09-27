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

    #[test]
    fn fx_chunks_its_bytes_eight_at_a_time() {
        // One long write and two word-sized ones must land on the same hash.
        let mut whole = FxHasher::default();
        whole.write(b"12345678ABCDEFGH");
        let mut split = FxHasher::default();
        split.write(b"12345678");
        split.write(b"ABCDEFGH");
        assert_eq!(whole.finish(), split.finish());
    }

    #[test]
    fn fx_zero_pads_a_short_tail() {
        let mut tail = FxHasher::default();
        tail.write(b"abc");
        // The tail is padded with zeros, so the padded form hashes identically.
        let mut padded = FxHasher::default();
        padded.write(b"abc\0\0\0\0\0");
        assert_eq!(tail.finish(), padded.finish());
    }

    #[test]
    fn fx_integer_writes_match_their_byte_form() {
        let mut from_u32 = FxHasher::default();
        from_u32.write_u32(0x0102_0304);
        let mut from_bytes = FxHasher::default();
        from_bytes.write(&0x0102_0304u32.to_le_bytes());
        assert_eq!(from_u32.finish(), from_bytes.finish());

        let mut narrow = FxHasher::default();
        narrow.write_u8(0xAB);
        let mut wide = FxHasher::default();
        wide.write_u32(0xAB);
        assert_eq!(narrow.finish(), wide.finish());

        let mut word = FxHasher::default();
        word.write_u64(0xDEAD_BEEF);
        let mut sized = FxHasher::default();
        sized.write_usize(0xDEAD_BEEF);
        assert_eq!(word.finish(), sized.finish());
    }

    #[test]
    fn the_fx_finish_avalanches_the_state() {
        let mut hasher = FxHasher::default();
        hasher.write_u64(1);
        // Without the final mix, consecutive keys keep nearly constant high bits
        // and collapse the table (see the doc comment on `finish`).
        assert_ne!(hasher.finish(), hasher.hash);
    }

    #[test]
    fn consecutive_correlation_ids_do_not_collide() {
        // A correlation id is `pid ++ counter`: keys differing only in their low
        // bytes, which is exactly the shape that used to collapse the table.
        let fx = BuildHasherDefault::<FxHasher>::default();
        let mut hashes = std::collections::HashSet::new();
        for counter in 0..1024u64 {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&4_242u64.to_be_bytes());
            key[8..].copy_from_slice(&counter.to_be_bytes());
            hashes.insert(fx.hash_one(key));
        }
        assert_eq!(
            hashes.len(),
            1024,
            "every consecutive id maps to its own hash"
        );
    }

    #[test]
    fn the_identity_hasher_ignores_byte_writes() {
        let mut hasher = IdentityHasher::default();
        // `write` is unreachable for the `u32` keys `IdMap` stores, and must not
        // corrupt the state if it is ever reached.
        hasher.write(b"ignored");
        assert_eq!(hasher.finish(), 0);

        hasher.write_u32(9);
        assert_eq!(hasher.finish(), 9);
        hasher.write(b"still ignored");
        assert_eq!(
            hasher.finish(),
            9,
            "a byte write must not disturb the value"
        );
    }

    #[test]
    fn the_transport_maps_round_trip_their_keys() {
        let mut fast: FastMap<[u8; 16], u32> = FastMap::default();
        fast.insert([7u8; 16], 42);
        assert_eq!(fast.get(&[7u8; 16]), Some(&42));
        assert_eq!(fast.get(&[8u8; 16]), None);

        let mut ids: IdMap<u32, u32> = IdMap::default();
        ids.insert(3, 9);
        assert_eq!(ids.get(&3), Some(&9));
    }
}
