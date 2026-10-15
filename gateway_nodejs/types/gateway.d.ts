/**
 * TypeScript binding template for an ice-rpc Node.js gateway addon.
 *
 * `napi` generates `index.d.ts`, which types the addon's surface generically
 * (`callService(service, method, args): Promise<any>`). This file adds a **simple
 * typed layer** on top: the event envelope, the call shape, and a `ServiceMap`
 * that names the services you actually serve and their methods.
 *
 * Copy this file into your project, replace `ServiceMap` with your services, and
 * add it to your `tsconfig` (or reference it) — the ambient module below then
 * types the addon's exports.
 *
 * The argument convention (see `docs/nodejs-gateway-api-v3.md`) is:
 *   0 argument  -> no argument
 *   1 argument  -> the value itself
 *   2+ arguments -> an object `{ name: value, ... }`
 *   1 `Vec<u8>`  -> a base64 string
 * A method with two or more arguments is therefore typed to take a single object.
 */

declare module 'gateway-nodejs' {
  /** A correlation id: a UUID-like hexadecimal string. */
  export type CorrelationId = string;

  /** One intermediate value of a stream: `emitNodejsEvent(cid, { data })`. */
  export interface Emitted<T = unknown> {
    data: T;
  }

  /**
   * The terminal answer of a call: `resolveNodejsCall(cid, …)`.
   *
   * The closing event is derived from the envelope, never stated:
   * - `{ data }` — close with one last value (a single `CompleteWith` sample);
   * - `{ error }` — close with a business error;
   * - `{}` — close without a value.
   */
  export type Terminal<T = unknown, E = unknown> =
    | { data: T }
    | { error: E }
    | { data?: undefined; error?: undefined };

  /** One call handed to the provider dispatcher. */
  export interface Call {
    correlationId: CorrelationId;
    service: string;
    method: string;
    /** A native JS value: no `JSON.parse` is involved. */
    args: unknown;
  }

  export function registerService(name: string): void;
  export function init(callback: (err: Error | null, call: Call) => void): void;
  export function callService(service: string, method: string, args?: unknown): Promise<unknown>;
  export function callServiceStream(
    service: string,
    method: string,
    args?: unknown,
  ): Promise<unknown[]>;
  export function emitNodejsEvent(correlationId: CorrelationId, event: Emitted): void;
  export function resolveNodejsCall(correlationId: CorrelationId, event: Terminal): void;
  export function shutdown(): Promise<void>;
  export function version(): string;

  // ── Per-service typings — replace with your services ──────────────────────

  /** The entry type of `ContextService::list`. */
  export interface ContextEntry {
    key: string;
    value: string;
  }

  /**
   * `ContextService`, as served by the reference addon.
   *
   * Each method mirrors the Rust declaration `-> Observable<Ok, Err>`: the
   * promise resolves with `Ok` and rejects with a coded `Error` for `Err`.
   */
  export interface ContextService {
    get(key: string): Promise<string>;
    set(key: string, value: string): Promise<boolean>;
    delete(key: string): Promise<boolean>;
    list(): Promise<ContextEntry>;
  }

  /** `DatabaseService`, as served by the reference addon. */
  export interface DatabaseService {
    get_user_age(name: string): Promise<number>;
  }

  /** The services this addon serves, keyed by logical name. */
  export interface ServiceMap {
    ContextService: ContextService;
    DatabaseService: DatabaseService;
  }

  export type ServiceName = keyof ServiceMap;
  export type MethodName<S extends ServiceName> = keyof ServiceMap[S] & string;
}

/**
 * A typed façade over `callService`.
 *
 * This is a **pattern**, not a runtime export of the addon: copy it into your
 * own `.ts` alongside the ambient module above. It forwards to the addon's
 * `callService`, but constrains `service`/`method` to the `ServiceMap` and infers
 * the argument and result types from the method signature.
 *
 * Because a `.d.ts` cannot hold an implementation, the body is shown here as a
 * comment; in your project it is a one-liner:
 *
 * ```ts
 * import * as raw from 'gateway-nodejs';
 * import type { ServiceMap, ServiceName, MethodName } from './gateway';
 *
 * export function callServiceTyped<S extends ServiceName, M extends MethodName<S>>(
 *   service: S,
 *   method: M,
 *   args?: Parameters<ServiceMap[S][M]>[0],
 * ): ReturnType<ServiceMap[S][M]> {
 *   return raw.callService(service, method, args) as ReturnType<ServiceMap[S][M]>;
 * }
 * ```
 *
 * Usage:
 *
 * ```ts
 * const age    = await callServiceTyped('DatabaseService', 'get_user_age', 'Alice');
 * const ok     = await callServiceTyped('ContextService', 'set', { key: 'k', value: 'v' });
 * const values = await callServiceStream('ContextService', 'list');
 * ```
 */
