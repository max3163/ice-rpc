//! Stable labels of the enums that cross a process boundary or feed the metrics.
//!
//! A label is a **contract**, not a debug string: an observer keys its series on
//! it (`kind="complete"`, `role="resp"`, `state="dead"`), so renaming one
//! silently breaks a dashboard. Writing them in a table per enum, in the same
//! shape everywhere, is what makes them auditable together — and what makes a
//! second, drifting table visible if one ever appears.

/// Implements `label()` on an enum from a table of `variant => "text"` pairs.
///
/// The mapping itself cannot be derived: a stable label is a naming decision, so
/// it has to be written somewhere. What this macro removes is everything around
/// the decision — the `impl`, the signature, the `match` and the exhaustiveness
/// — so that declaring the labels of an enum is writing a table, and adding a
/// variant without a label does not compile.
macro_rules! impl_labels {
    ($enum:ty { $($variant:path => $text:literal),+ $(,)? }) => {
        impl $enum {
            /// Stable label used by the metrics and the traces.
            ///
            /// Part of the observable contract: an observer keys its series on
            /// this string, so renaming one changes what a dashboard sees.
            #[inline]
            pub const fn label(self) -> &'static str {
                match self {
                    $($variant => $text,)+
                }
            }
        }
    };
}

pub(crate) use impl_labels;

#[cfg(test)]
mod tests {
    /// Stands for the enums the workspace labels: the macro only needs a type to
    /// attach the method to and a table to read.
    #[derive(Clone, Copy)]
    enum Sample {
        First,
        Second,
    }

    impl_labels!(Sample {
        Sample::First => "first",
        Sample::Second => "second",
    });

    #[test]
    fn the_table_becomes_the_label_method() {
        assert_eq!(Sample::First.label(), "first");
        assert_eq!(Sample::Second.label(), "second");
    }
}
