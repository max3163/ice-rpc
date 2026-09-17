# Golden expansions

One file per `#[service]` declaration, holding the **pretty-printed** expansion of
the macro. They are compared by `src/golden_tests.rs`.

There are two sets, one per feature state, because the `monitoring` feature adds
the `{Trait}Decoder` and the generated `Display` implementation to the same
expansion:

| File | Compared by |
|---|---|
| `<base>.rs` | `cargo test -p ice-rpc-macros` (default features) |
| `<base>.monitoring.rs` | `cargo test -p ice-rpc-macros --all-features` |

Both are committed and both are compared in their own configuration, so neither
build can drift unnoticed.

They exist for one reason: when a call misbehaves, the code to read is the
generated one, and the generator is spread over eight modules. A `to_string()`
comparison would be a single line of several thousand characters — nothing a
diff can show. So each expansion is parsed back and rendered by `prettyplease`
before being compared, which makes a generator change reviewable.

Regenerate them after an intended change — the two commands, one per set:

```bash
ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros
ICE_RPC_BLESS=1 cargo test -p ice-rpc-macros --all-features
```

On a mismatch the test also writes the current output next to the golden as
`<name>.actual.rs` (git-ignored), so `diff` is enough to see what moved; nothing
is rewritten unless `ICE_RPC_BLESS` is set.

These files are **not** rustfmt output and must not be reformatted by hand:
`cargo fmt` ignores them (they are data, not targets), and running `rustfmt` over
the directory would replace the reference with a slightly different formatting and
break every test.

A `.actual.rs` file must never be committed — delete it once the diff is read.
Ten `.actual.rs` files in a row mean the test is being bypassed instead of fixed.
