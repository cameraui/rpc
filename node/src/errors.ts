import type { ErrorCode, RPCError } from './types.js';

const LOCAL_NO_RESPONDERS = Symbol('rpc.localNoResponders');

/**
 * RPC Exception class for structured error handling
 */
export class RPCException extends Error {
  public readonly code: ErrorCode | string;
  public readonly data?: any;

  constructor(code: ErrorCode | string, message: string, data?: any) {
    super(message);
    this.name = 'RPCException';
    this.code = code;
    this.data = data;
  }

  /**
   * Create exception from JSON error object
   */
  static fromJSON(error: RPCError): RPCException {
    return new RPCException(error.code, error.message, error.data);
  }

  /**
   * Convert exception to JSON-serializable format
   */
  public toJSON(): RPCError {
    return {
      code: this.code,
      message: this.message,
      data: this.data,
    };
  }
}

/**
 * Helper function to create RPC exceptions
 */
export function createError(code: ErrorCode | string, message: string, data?: any): RPCException {
  return new RPCException(code, message, data);
}

/**
 * Type guard to check if error is an RPC exception
 */
export function isRPCError(error: any): error is RPCException {
  return error instanceof RPCException;
}

/**
 * Mark an error this client raised for a no-responder status it saw itself.
 * A handler that passes such an error on reports `upstream unavailable`
 * instead, so its caller does not take it for a missing responder of its own.
 *
 * Internal: client.ts and handler.ts use it.
 */
export function markLocalNoResponders<E extends Error>(error: E, subject: string): E {
  Object.defineProperty(error, LOCAL_NO_RESPONDERS, { value: subject });
  return error;
}

/**
 * The subject of a no-responder error from markLocalNoResponders(),
 * undefined for any other value.
 *
 * Internal: handler.ts uses it.
 */
export function localNoRespondersSubject(error: unknown): string | undefined {
  if (error === null || typeof error !== 'object') return undefined;
  const subject = (error as { [LOCAL_NO_RESPONDERS]?: unknown })[LOCAL_NO_RESPONDERS];
  return typeof subject === 'string' ? subject : undefined;
}

/**
 * Check whether an error is a NATS "no responders" error — i.e. nobody is
 * subscribed to the requested subject (the target service is not running).
 */
export function isNoRespondersError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  if ((error as { name?: string }).name === 'NoRespondersError') return true;
  const message = (error as { message?: string }).message;
  return typeof message === 'string' && message.includes('no responders');
}
