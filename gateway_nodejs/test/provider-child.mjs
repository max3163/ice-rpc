/**
 * The provider half of `roundtrip.test.mjs`, run as a **separate process**.
 *
 * It has to be a separate process: within one process the `ServiceLocator`
 * hands back the instance it registered, i.e. the `ProviderJson` proxy, and
 * calling a method on it answers
 * `ProviderJson: direct calls are not supported — use IPC`. A process can
 * therefore not consume a service it provides, which is exactly what makes a
 * real round trip need two of them.
 *
 * Prints `PROVIDER_READY` on stdout once the service should be announced, then
 * stays alive until the parent kills it.
 */

import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const gateway = require('../index.js');

const STORE = new Map([
  ['app.name', 'ice-rpc-gateway'],
  ['app.version', '0.3.2'],
]);

gateway.registerService('ContextService');
// A service whose method streams several values before completing: it is what
// `callServiceStream` and `emitNodejsEvent` exist for.
gateway.registerService('NotificationService');

gateway.init((err, call) => {
  if (err) {
    return;
  }
  if (call.service === 'NotificationService' && call.method === 'watch') {
    const count = typeof call.args === 'number' ? call.args : 3;
    // One call, several events: each one becomes its own wire sample.
    for (let value = 1; value <= count; value += 1) {
      gateway.emitNodejsEvent(call.correlationId, { type: 'next', data: value });
    }
    // `resolveNodejsCall` is the terminal event; it also closes the call.
    gateway.resolveNodejsCall(call.correlationId, { type: 'complete' });
    return;
  }
  const value = STORE.get(call.args);
  gateway.resolveNodejsCall(
    call.correlationId,
    value === undefined
      ? { type: 'error', data: 'KeyNotFound' }
      : { type: 'next', data: value },
  );
});

// `init` announces the registered services in the background and exposes no
// completion signal, so the pause is the contract with the parent process:
// long enough for the requester's subscriber to be connected.
setTimeout(() => {
  process.stdout.write('PROVIDER_READY\n');
}, 1500);

process.on('SIGTERM', () => {
  gateway.shutdown().finally(() => process.exit(0));
});

// Keep the event loop (and therefore the JS dispatcher) alive.
setInterval(() => {}, 1 << 30);
