//! Tests for the combining operators.

use crate::transform::test_support::{drain, single};
use crate::Event;

#[test]
fn start_with_prefixes_initial_value() {
    let stream = single(1).start_with(0);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Event::Next(v) if *v == 0));
    assert!(matches!(&events[1], Event::Next(v) if *v == 1));
    assert!(matches!(&events[2], Event::Complete));
}
