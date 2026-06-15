// Main client
export { createRPCClient } from './client.js';

// Channel/PrivateChannel for bidirectional communication
export { Channel, PrivateChannel } from './channel.js';

// Error handling
export { isNoRespondersError, isRPCError, RPCException } from './errors.js';
export { ERROR_CODES } from './types.js';

// Service support
export { RPCService, Service } from './service.js';

// Decorators
export { RPCClass, RPCMethod, RPCNested, RPCProperty } from './decorators.js';

// Pull-iterator-with-callbacks helper
export { rpcCallbacks } from './utils.js';

// Core types that users need
export type {
  CallbackInvocation,
  CallbackMessage,
  CallbackParams,
  ErrorCode,
  PullCallbackParams,
  RPCAuthOptions,
  RPCClient,
  RPCClientOptions,
  RPCError,
} from './types.js';

// Advanced types (for users who need them)
export type { Promisify } from './types.js';

// NATS service types
export type { ServiceConfig, ServiceInfo, ServiceStats } from '@nats-io/services';

export type * from '@nats-io/nats-core';
