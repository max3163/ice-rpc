/**
 * Shared plumbing of the two-process Node.js checks.
 *
 * `e2e-roundtrip.mjs` and `bench-bridge.mjs` both need the same three things:
 * a provider running in its own process, a way to fail rather than hang, and a
 * result summary that sets the exit code. They live here so the two scripts say
 * only what is specific to them.
 */

import { spawn } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

/// How long the provider is given to announce itself.
export const PROVIDER_TIMEOUT_MS = 20_000;
/// How long a single call may take before it is declared hung.
export const STEP_TIMEOUT_MS = 5_000;
/// How long the provider is given to shut down cleanly before being killed.
const GRACEFUL_EXIT_MS = 5_000;

/**
 * Starts `provider-child.mjs` and waits for its readiness marker.
 *
 * Two processes are required, not a convenience: within one process the
 * `ServiceLocator` returns the instance it registered (the `ProviderJson`
 * proxy), whose methods answer "direct calls are not supported — use IPC", so a
 * process cannot consume a service it provides.
 */
export async function startProvider() {
  const child = spawn(process.execPath, [join(HERE, 'provider-child.mjs')], {
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  // iceoryx2 logs on stderr are noise here; the readiness marker is on stdout.
  child.stderr.on('data', () => {});

  await new Promise((resolve, reject) => {
    let output = '';
    const timer = setTimeout(() => {
      reject(new Error(`the provider never became ready; output: ${output}`));
    }, PROVIDER_TIMEOUT_MS);
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      output += chunk;
      if (output.includes('PROVIDER_READY')) {
        clearTimeout(timer);
        resolve();
      }
    });
    child.on('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`the provider exited early (${code}); output: ${output}`));
    });
  });

  return child;
}

/**
 * Stops the provider.
 *
 * `SIGTERM` first: the child shuts its gateway down, which is what keeps the
 * shared memory reusable. `SIGKILL` leaves stale services behind and makes the
 * *following* run abort while creating its node — the remedy for an already
 * poisoned root is `scripts/purge-iceoryx2-root.sh --yes`.
 */
export async function stopProvider(child) {
  if (!child || child.exitCode !== null) {
    return;
  }
  const exited = new Promise((resolve) => child.on('exit', resolve));
  child.kill('SIGTERM');
  const exitedCleanly = await Promise.race([
    exited.then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), GRACEFUL_EXIT_MS)),
  ]);
  if (!exitedCleanly) {
    child.kill('SIGKILL');
    await exited;
  }
}

/** Rejects the returned promise if it does not settle within `ms`. */
export function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} did not settle within ${ms} ms`)), ms),
    ),
  ]);
}

/**
 * Runs `[name, body]` pairs, printing `ok`/`FAIL` for each.
 *
 * A failing step does not hide the ones after it, and the process exit code is
 * left at 0 only when every step passed.
 */
export async function runChecks(checks) {
  const results = [];
  for (const [name, body] of checks) {
    const started = process.hrtime.bigint();
    try {
      await body();
      const ms = Number(process.hrtime.bigint() - started) / 1e6;
      results.push({ name, ok: true });
      console.log(`ok    ${name}  (${ms.toFixed(1)} ms)`);
    } catch (error) {
      results.push({ name, ok: false });
      console.error(`FAIL  ${name}\n        ${error && error.message ? error.message : error}`);
    }
  }

  const failed = results.filter((result) => !result.ok).length;
  console.log(`\n${results.length - failed}/${results.length} checks passed`);
  if (failed !== 0) {
    process.exitCode = 1;
  }
  return results;
}
