//! Tests for pipelines that cross operator categories.

use crate::transform::test_support::{drain, local};
use crate::Event;

#[test]
fn map_filter_take_pipeline() {
    let stream = local(1..6).filter(|v| *v % 2 == 1).map(|v| v * 10).take(3);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 10));
    assert!(matches!(&events[1], Event::Next(v) if *v == 30));
    assert!(matches!(&events[2], Event::Next(v) if *v == 50));
    assert!(matches!(&events[3], Event::Complete));
}
