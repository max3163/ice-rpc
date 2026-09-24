/**
 * End-to-end round trip through the addon: a JavaScript provider in one process
 * answers a JavaScript consumer in another.
 *
 * This is the only suite that lets the IPC bus carry the call. The converter
 * tests are pure functions and the generated consumer contract is exercised
 * against a `ProviderJson` proxy that answers without a bus — both on purpose,
 * so they stay fast and deterministic. Neither proves that a JavaScript provider
 * really answers a JavaScript consumer.
 *
 * It is a plain script rather than a `node:test` file on purpose: the test runner
 * spawns each test file itself, and it treats **every** file of a `test/`
 * directory as a test, so it would also run `provider-child.mjs`, which never
 * terminates. Running this file directly keeps the nesting at one level and the
 * failure mode readable. `npm run test:e2e`.
 *
 * Requires a build: `npm run build:debug` (or `npm run build`).
 *
 * Before the first run on a machine whose shared memory was left behind by a
 * process that was killed, run `scripts/purge-iceoryx2-root.sh --yes` first:
 * iceoryx2 aborts while creating its node, and the only symptom is a process
 * that exits with a bare `stdout is not a tty` and nothing else.
 */

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

import {
  STEP_TIMEOUT_MS,
  runChecks,
  startProvider,
  stopProvider,
  withTimeout,
} from './harness.mjs';

const require = createRequire(import.meta.url);
const gateway = require('../index.js');

async function main() {
  const provider = await startProvider();

  try {
    // Consumer side: nothing to dispatch, but the bridge must exist for the
    // reverse direction of the calls this process makes.
    gateway.init(() => {});

    await runChecks([
      [
        'a JavaScript provider answers a JavaScript consumer through real IPC',
        async () => {
          const started = Date.now();
          const value = await withTimeout(
            gateway.callService('ContextService', 'get', 'app.name'),
            STEP_TIMEOUT_MS,
            'the first call',
          );
          assert.equal(value, 'ice-rpc-gateway', 'the answer must come from the other process');
          assert.ok(Date.now() - started < STEP_TIMEOUT_MS, 'a served call must answer promptly');
        },
      ],
      [
        'a second call goes through the bus again',
        async () => {
          const value = await withTimeout(
            gateway.callService('ContextService', 'get', 'app.version'),
            STEP_TIMEOUT_MS,
            'the second call',
          );
          assert.equal(value, '0.3.2');
        },
      ],
      [
        'a business error travels back as a coded failure',
        async () => {
          await assert.rejects(
            () => gateway.callService('ContextService', 'get', 'missing.key'),
            (error) => error.message.startsWith('E_BUSINESS'),
            'the provider answered KeyNotFound, which must surface as E_BUSINESS',
          );
        },
      ],
      [
        'arguments that do not fit the declaration are refused',
        async () => {
          await assert.rejects(
            () => gateway.callService('ContextService', 'set', 'only-one-argument'),
            (error) => error.message.startsWith('E_INVALID_ARGS'),
            'set takes (key, value), so a single argument must be refused',
          );
        },
      ],
      [
        'a multi-value service streams every value to callServiceStream',
        async () => {
          const values = await withTimeout(
            gateway.callServiceStream('NotificationService', 'watch', 3),
            STEP_TIMEOUT_MS,
            'the streaming call',
          );
          assert.deepEqual(values, [1, 2, 3], 'every emitted value must be collected in order');
        },
      ],
      [
        'callService answers with the first value of the same stream',
        async () => {
          const first = await withTimeout(
            gateway.callService('NotificationService', 'watch', 3),
            STEP_TIMEOUT_MS,
            'the first-value call',
          );
          assert.equal(first, 1, 'the first value must answer without waiting for the stream');
        },
      ],
      [
        'an undeclared method is reported as E_UNKNOWN_METHOD',
        async () => {
          await assert.rejects(
            () => gateway.callService('ContextService', 'nope', 'x'),
            (error) => error.message.startsWith('E_UNKNOWN_METHOD'),
          );
        },
      ],
      [
        'an undeclared service is reported as E_UNKNOWN_SERVICE',
        async () => {
          await assert.rejects(
            () => gateway.callService('NoSuchService', 'get', 'x'),
            (error) => error.message.startsWith('E_UNKNOWN_SERVICE'),
          );
        },
      ],
    ]);
  } finally {
    await withTimeout(gateway.shutdown(), STEP_TIMEOUT_MS, 'shutdown').catch(() => {});
    await stopProvider(provider);
  }
}

main().catch((error) => {
  console.error(`fatal: ${error && error.message ? error.message : error}`);
  process.exitCode = 1;
});
