//! Tests for the conditional operators.

use crate::transform::test_support::{drain, is_technical, local};
use crate::Event;

#[test]
fn take_until_emits_cancelled_when_token_fires() {
    let token = crate::CancellationToken::new();
    token.cancel();
    let stream = local([1, 2, 3]).take_until(&token);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 1);
    assert!(is_technical(&events[0]));
}

#[test]
fn take_until_forwards_values_when_not_cancelled() {
    let token = crate::CancellationToken::new();
    let stream = local([1, 2, 3]).take_until(&token);

    let events = pollster::block_on(drain(stream));
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Next(v) if *v == 1));
    assert!(matches!(&events[1], Event::Next(v) if *v == 2));
    assert!(matches!(&events[2], Event::Next(v) if *v == 3));
    assert!(matches!(&events[3], Event::Complete));
}
