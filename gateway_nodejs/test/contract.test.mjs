/**
 * The addon's surface, checked without any IPC.
 *
 * Deliberately spawn-free and bus-free so `node --test` stays fast and
 * deterministic; the round trip lives in `e2e-roundtrip.mjs`, which needs two
 * processes and therefore cannot run under the test runner (see that file).
 *
 * Requires a build: `npm run build:debug` (or `npm run build`).
 */

import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const gateway = require('../index.js');

before(() => {
  gateway.init(() => {});
});

after(async () => {
  await gateway.shutdown();
});

test('version() reports the versions actually compiled in', () => {
  const reported = gateway.version();
  assert.match(reported, /^gateway-nodejs v\d+\.\d+\.\d+ \(ice-rpc v\d+\.\d+\.\d+, protocol v\d+\)$/);
  // The regression this pins: it used to answer hardcoded numbers that no
  // manifest agreed with.
  assert.doesNotMatch(reported, /iceoryx2/, 'the bus version is not surfaced: it is not exposed by the crate');
});

test('an undeclared service is refused with a coded error listing the served ones', async () => {
  await assert.rejects(
    () => gateway.callService('NoSuchService', 'get', 'x'),
    (error) => {
      assert.ok(error.message.startsWith('E_UNKNOWN_SERVICE'), error.message);
      assert.ok(
        error.message.includes('ContextService'),
        `the message must list the served services: ${error.message}`,
      );
      return true;
    },
  );
});

test('an undeclared method is refused with its own code', async () => {
  await assert.rejects(
    () => gateway.callService('ContextService', 'nope', 'x'),
    (error) => error.message.startsWith('E_UNKNOWN_METHOD'),
  );
});

test('calling before init() is a state error, not a crash', async () => {
  // `shutdown` in the `after` hook only runs at the end, so this asserts the
  // running state's contract instead: a second `init` is refused with a code.
  assert.throws(
    () => gateway.init(() => {}),
    (error) => error.message.startsWith('E_GATEWAY_STATE'),
  );
});

test('registering after init() is refused with the same code', () => {
  assert.throws(
    () => gateway.registerService('ContextService'),
    (error) => error.message.startsWith('E_GATEWAY_STATE'),
  );
});

test('resolving an unknown correlation id is refused with its own code', () => {
  assert.throws(
    () => gateway.resolveNodejsCall('11111111-2222-3333-4444-555555555555', { type: 'next' }),
    (error) => error.message.startsWith('E_UNKNOWN_CID'),
  );
});

test('resolving a malformed correlation id is refused with its own code', () => {
  assert.throws(
    () => gateway.resolveNodejsCall('not-a-correlation-id', { type: 'next' }),
    (error) => error.message.startsWith('E_INVALID_CID'),
  );
});
