/**
 * A **multi-value** service served from JavaScript.
 *
 * `NotificationService::watch(count)` emits `count` values and then completes.
 * On the wire each value is its own sample, so serving it from JavaScript means
 * answering one call with several events:
 *
 *   `emitNodejsEvent(cid, { type: 'next', data })`  — pushes a value, keeps the call open
 *   `resolveNodejsCall(cid, { type: 'complete' })`  — closes the call
 *
 * A consumer reads them with `callServiceStream` (every value) or
 * `callService` (the first one, without waiting for the end — which is what an
 * endless stream needs).
 *
 * Usage:
 *   npm run build
 *   node examples/provider-notification.js
 */

const gateway = require('../index.js');

const WATCH_INTERVAL_MS = 100;

/**
 * Streams `count` values, one every `WATCH_INTERVAL_MS`, then completes.
 *
 * Asynchronous on purpose: `emitNodejsEvent` is called from a timer, so the
 * call stays open across several turns of the event loop. The gateway does not
 * block waiting for it.
 */
function handleWatch(correlationId, count) {
  let emitted = 0;
  const tick = () => {
    if (emitted >= count) {
      gateway.resolveNodejsCall(correlationId, { type: 'complete' });
      return;
    }
    emitted += 1;
    gateway.emitNodejsEvent(correlationId, { type: 'next', data: emitted });
    setTimeout(tick, WATCH_INTERVAL_MS);
  };
  tick();
}

function dispatch(err, call) {
  if (err) {
    console.error(`dispatch error: ${err.message ?? err}`);
    return;
  }
  const { correlationId, service, method, args } = call;
  if (service === 'NotificationService' && method === 'watch') {
    handleWatch(correlationId, typeof args === 'number' ? args : 3);
    return;
  }
  if (service === 'NotificationService' && method === 'ping') {
    // A single-value method: one `next` is enough, and `resolveNodejsCall`
    // closes the call.
    gateway.resolveNodejsCall(correlationId, { type: 'next', data: 1 });
    return;
  }
  gateway.resolveNodejsCall(correlationId, {
    type: 'error',
    data: `this process does not provide '${service}::${method}'`,
  });
}

async function main() {
  console.log('=== Node.js provider of a multi-value service ===\n');

  gateway.registerService('NotificationService');
  gateway.init(dispatch);
  console.log('providing NotificationService::watch(count)\n');

  console.log('Consuming our own values from another process is the normal setup.');
  console.log('From here, `watch` is served as N samples then a Complete.\n');
  console.log('Serving requests (Ctrl+C to quit)...');

  let stopping = false;
  const stop = async () => {
    if (stopping) {
      return;
    }
    stopping = true;
    await gateway.shutdown();
    process.exit(0);
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}

main().catch((error) => {
  console.error(`fatal: ${error.message ?? error}`);
  process.exit(1);
});
