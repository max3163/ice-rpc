/**
 * The Node.js gateway, provider **and** consumer side.
 *
 * This process provides `ContextService` (its business logic is JavaScript) and,
 * at the same time, consumes `DatabaseService` from a Rust provider — start
 * `cargo make provider` first, or the calls below will report `E_NO_PROVIDER`.
 *
 * It cannot consume the service it provides: `ice_rpc::locator()` hands back the
 * instance registered by `registerService`, whose methods are served over IPC,
 * not called directly. A service that needs its own is a two-process design.
 *
 * Usage:
 *   npm run build        # once, to produce the addon
 *   node examples/provider-context.js
 *
 * See the crate `Readme.md` for the full surface and
 * `docs/nodejs-gateway-api-v2.md` for the contract.
 */

const gateway = require('../index.js');

const store = new Map([
  ['app.name', 'ice-rpc-gateway'],
  ['app.version', '1.0.0'],
  ['app.environment', 'development'],
  ['session.timeout', '3600'],
]);

/** Answers one `ContextService` call from the in-process store. */
function handleContext(method, args, correlationId) {
  switch (method) {
    case 'get': {
      const value = store.get(args);
      // A terminal event closes the call; `type` may only be `next`, `complete`
      // or `error`, and the payload must fit the declared error type.
      return value === undefined
        ? { type: 'error', data: 'KeyNotFound' }
        : { type: 'next', data: value };
    }
    case 'set':
      // `set` takes two parameters, so JavaScript receives an object.
      store.set(String(args.key), String(args.value));
      return { type: 'next', data: true };
    case 'delete':
      return { type: 'next', data: store.delete(args) };
    case 'list': {
      const entries = [...store].map(([key, value]) => ({ key, value }));
      return entries.length === 0
        ? { type: 'complete' }
        : { type: 'next', data: entries[0] };
    }
    default:
      return { type: 'error', data: `unknown method '${method}'` };
  }
}

/**
 * The single dispatcher, called for every request of every registered service.
 *
 * The signature is `(err, call)` — the Rust side builds the ThreadsafeFunction
 * with `callee_handled::<true>()`. `call.args` is a native JS value: no
 * `JSON.parse` is involved.
 */
function dispatch(err, call) {
  if (err) {
    console.error(`dispatch error: ${err.message ?? err}`);
    return;
  }
  const { correlationId, service, method, args } = call;
  if (process.env.DEBUG) {
    console.log(`${service}::${method}  cid=${correlationId}  args=${JSON.stringify(args)}`);
  }

  const event =
    service === 'ContextService'
      ? handleContext(method, args, correlationId)
      : { type: 'error', data: `this process does not provide '${service}'` };

  // `resolveNodejsCall` sends the terminal event and closes the call. Use
  // `emitNodejsEvent` first when a method streams several values.
  gateway.resolveNodejsCall(correlationId, event);
}

/** Calls the Rust provider, reporting the coded failures instead of crashing. */
async function demoConsumerCalls() {
  console.log('\nCalling the Rust `DatabaseService` provider:');
  for (const name of ['Alice', 'Bob', 'Nobody']) {
    const started = Date.now();
    try {
      const age = await gateway.callService('DatabaseService', 'get_user_age', name);
      console.log(`  ok    get_user_age("${name}") -> ${age}  (${Date.now() - started} ms)`);
    } catch (error) {
      // The message starts with a stable code: `E_NO_PROVIDER`, `E_BUSINESS`, …
      console.log(`  error get_user_age("${name}") -> ${error.message}`);
    }
  }
}

async function main() {
  console.log('=== ice-rpc Node.js gateway: provider + consumer ===\n');

  // Only the services THIS process implements are registered. Consumers are
  // created on demand by the service locator, nothing to declare.
  gateway.registerService('ContextService');

  // `init` throws on failure; there is no boolean to check any more.
  gateway.init(dispatch);
  console.log('gateway initialized (providing ContextService)');

  // Opt-in: with no Rust provider running, each call below waits out the
  // transport retry window (~30 s) before reporting `E_NO_PROVIDER`.
  if (process.argv.includes("--consume")) {
    await demoConsumerCalls();
  }

  console.log('\nServing ContextService requests (Ctrl+C or q to quit)...');

  let stopping = false;
  const stop = async () => {
    if (stopping) {
      return;
    }
    stopping = true;
    console.log('\nshutting down...');
    if (process.stdin.isTTY) {
      try {
        process.stdin.setRawMode(false);
      } catch {
        // Not a TTY after all: nothing to restore.
      }
      process.stdin.pause();
    }
    // `shutdown` returns a promise: it resolves once the IPC resources are
    // released, which is what lets the process exit cleanly.
    await gateway.shutdown();
    console.log('gateway stopped.');
    process.exit(0);
  };

  const readline = require('node:readline');
  readline.emitKeypressEvents(process.stdin);
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(true);
  }
  process.stdin.on('keypress', (str, key) => {
    if ((key && key.ctrl && key.name === 'c') || (key && key.name === 'q')) {
      stop();
    }
  });
  process.stdin.resume();

  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  process.on('uncaughtException', (error) => {
    console.error('uncaught exception:', error);
    stop();
  });
}

main().catch((error) => {
  console.error(`fatal: ${error.message ?? error}`);
  process.exit(1);
});
