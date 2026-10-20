import type { ErrorCode, RPCError } from './types.js';

const LOCAL_NO_RESPONDERS = Symbol('rpc.localNoResponders');

export class RPCException extends Error {
  public readonly code: ErrorCode | string;
  public readonly data?: any;

  constructor(code: ErrorCode | string, message: string, data?: any) {
    super(message);
    this.name = 'RPCException';
    this.code = code;
    this.data = data;
  }

  static fromJSON(error: RPCError): RPCException {
    return new RPCException(error.code, error.message, error.data);
  }

  public toJSON(): RPCError {
    return {
      code: this.code,
      message: this.message,
      data: this.data,
    };
  }
}

export function createError(code: ErrorCode | string, message: string, data?: any): RPCException {
  return new RPCException(code, message, data);
}

export function isRPCError(error: any): error is RPCException {
  return error instanceof RPCException;
}

export function markLocalNoResponders<E extends Error>(error: E, subject: string): E {
  Object.defineProperty(error, LOCAL_NO_RESPONDERS, { value: subject });
  return error;
}

export function localNoRespondersSubject(error: unknown): string | undefined {
  if (error === null || typeof error !== 'object') return undefined;
  const subject = (error as { [LOCAL_NO_RESPONDERS]?: unknown })[LOCAL_NO_RESPONDERS];
  return typeof subject === 'string' ? subject : undefined;
}

export function isNoRespondersError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  if ((error as { name?: string }).name === 'NoRespondersError') return true;
  const message = (error as { message?: string }).message;
  return typeof message === 'string' && message.includes('no responders');
}
