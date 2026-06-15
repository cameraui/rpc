import { describe, expect, it } from 'vitest';

import { createError, isNoRespondersError, isRPCError, RPCException } from '../src/errors.js';
import { ERROR_CODES } from '../src/types.js';

describe('RPCException', () => {
  it('exposes code, message and data', () => {
    const err = new RPCException(ERROR_CODES.INVALID_PARAMS, 'bad params', { field: 'name' });
    expect(err.code).toBe(ERROR_CODES.INVALID_PARAMS);
    expect(err.message).toBe('bad params');
    expect(err.data).toEqual({ field: 'name' });
  });

  it('is an Error with the RPCException name', () => {
    const err = new RPCException(ERROR_CODES.INTERNAL_ERROR, 'boom');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('RPCException');
  });

  it('allows undefined data', () => {
    const err = new RPCException(ERROR_CODES.TIMEOUT, 'timed out');
    expect(err.data).toBeUndefined();
  });

  it('serializes to a JSON error object', () => {
    const err = new RPCException(ERROR_CODES.NOT_FOUND, 'missing', { id: 7 });
    expect(err.toJSON()).toEqual({ code: ERROR_CODES.NOT_FOUND, message: 'missing', data: { id: 7 } });
  });

  it('reconstructs from a JSON error object', () => {
    const restored = RPCException.fromJSON({ code: ERROR_CODES.STREAM_ERROR, message: 'stream failed', data: 99 });
    expect(restored).toBeInstanceOf(RPCException);
    expect(restored.code).toBe(ERROR_CODES.STREAM_ERROR);
    expect(restored.message).toBe('stream failed');
    expect(restored.data).toBe(99);
  });

  it('survives a toJSON / fromJSON round-trip', () => {
    const original = new RPCException(ERROR_CODES.PAYLOAD_TOO_LARGE, 'too big', { size: 1024 });
    const restored = RPCException.fromJSON(original.toJSON());
    expect(restored.toJSON()).toEqual(original.toJSON());
  });
});

describe('createError', () => {
  it('produces an RPCException', () => {
    const err = createError(ERROR_CODES.METHOD_NOT_FOUND, 'no method');
    expect(err).toBeInstanceOf(RPCException);
    expect(err.code).toBe(ERROR_CODES.METHOD_NOT_FOUND);
    expect(err.message).toBe('no method');
  });
});

describe('isRPCError', () => {
  it('returns true for an RPCException', () => {
    expect(isRPCError(new RPCException(ERROR_CODES.INTERNAL_ERROR, 'x'))).toBe(true);
  });

  it('returns false for a plain Error', () => {
    expect(isRPCError(new Error('plain'))).toBe(false);
  });

  it('returns false for non-error values', () => {
    expect(isRPCError(null)).toBe(false);
    expect(isRPCError(undefined)).toBe(false);
    expect(isRPCError('error')).toBe(false);
    expect(isRPCError({ code: 'x', message: 'y' })).toBe(false);
  });
});

describe('isNoRespondersError', () => {
  it('returns true when the error name is NoRespondersError', () => {
    expect(isNoRespondersError({ name: 'NoRespondersError' })).toBe(true);
  });

  it('returns true when the message mentions no responders', () => {
    expect(isNoRespondersError(new Error('503: no responders available'))).toBe(true);
  });

  it('returns false for unrelated errors', () => {
    expect(isNoRespondersError(new Error('timeout'))).toBe(false);
  });

  it('returns false for non-object values', () => {
    expect(isNoRespondersError(null)).toBe(false);
    expect(isNoRespondersError(undefined)).toBe(false);
    expect(isNoRespondersError('no responders')).toBe(false);
  });
});

describe('ERROR_CODES', () => {
  it('maps each key to itself as a string value', () => {
    for (const [key, value] of Object.entries(ERROR_CODES)) {
      expect(value).toBe(key);
    }
  });

  it('includes the documented error codes', () => {
    expect(ERROR_CODES).toMatchObject({
      METHOD_NOT_FOUND: 'METHOD_NOT_FOUND',
      INVALID_PARAMS: 'INVALID_PARAMS',
      INTERNAL_ERROR: 'INTERNAL_ERROR',
      TIMEOUT: 'TIMEOUT',
      CONNECTION_CLOSED: 'CONNECTION_CLOSED',
      STREAM_ERROR: 'STREAM_ERROR',
      PAYLOAD_TOO_LARGE: 'PAYLOAD_TOO_LARGE',
      NOT_FOUND: 'NOT_FOUND',
    });
  });
});
