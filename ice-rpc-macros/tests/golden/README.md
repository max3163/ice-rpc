# Golden expansions

One file per `#[service]` declaration, holding the **pretty-printed** expansion of
the macro. They are compared by `src/golden_tests.rs`.

## Three pinned feature sets

The optional blocks of the expansion follow values, not `cfg!`: the set is passed
to `expand_service_with` (see `src/features.rs`). The test therefore expands the
same trait with the same three sets **in every build**, whatever features that
build happens to enable:

| Set | File | What it contains |
|---|---|---|
| none | `<base>.rs` | the client, server, proxy, lifecycle — nothing optional |
| `monitoring` | `<base>.monitoring.rs` | the above, plus the `{Trait}Decoder` and the generated `Display` |
| everything | `<base>.all.rs` | the above, plus the Node.js provider converters and the `JsonInvoker` implementation |

The three sets cover each optional block at least once, and the blocks are
independent. The eight combinations are checked separately, by assertion, in
`every_optional_block_follows_its_own_flag` — a golden per combination would be
eight files, of which a build can only ever check the one it has.

Reading `cfg!` in the test instead would have made it compare a *different*
reference depending on which other crate of the workspace enabled which feature,
since Cargo unifies features per build.

## Regenerating

After an intended change, one command regenerates all six files:

```bash
ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros
```

On a mismatch the test also writes the current output next to the golden as
`<name>.actual.rs` (git-ignored), so `diff` is enough to see what moved; nothing
is rewritten unless `ICE_RPC_BLESS` is set.

A `.actual.rs` file must never be committed — delete it once the diff is read.
Several `.actual.rs` files in a row mean the test is being bypassed instead of
fixed.

## What the size difference measures

The minimal set is what a plain Rust service pays for. The delta with `.all.rs` is
the code a deployment that never speaks Node.js or HTTP no longer compiles:

| Reference | `calculator` | `database` |
|---|---|---|
| `<base>.rs` | 286 lines | 369 lines |
| `<base>.monitoring.rs` | 358 | 450 |
| `<base>.all.rs` | 638 | 905 |

These files are **not** rustfmt output and must not be reformatted by hand:
`cargo fmt` ignores them (they are data, not targets), and running `rustfmt` over
the directory would replace the reference with a slightly different formatting and
break every test.
