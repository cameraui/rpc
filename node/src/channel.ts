import type { RPCClient } from './client.js';

export interface ChannelMessage {
  type: 'message' | 'close' | 'error';
  data?: any;
  error?: string;
  sender?: string; // For private channels
}

/**
 * Bidirectional communication channel between RPC clients
 */
export class Channel {
  protected closed = false;
  protected unsubscribe?: () => void;
  protected initialized = false;

  // Return type is `void` (consumer-void) so handlers that incidentally
  // return a value type-check. Runtime uses isPromise() on the actual
  // returned value to detect async handlers and chain them.
  private handlers = new Map<string, Set<(data: any) => void>>();
  private closeHandlers = new Set<() => void>();
  private errorHandlers = new Set<(error: Error) => void>();
  private subscriptions: Promise<() => void>[] = [];

  /**
   * Check if channel is closed
   */
  get isClosed(): boolean {
    return this.closed;
  }

  /**
   * Get the channel ID
   */
  get id(): string {
    return this.channelId;
  }

  constructor(
    protected client: RPCClient,
    protected channelId: string,
  ) {}

  /**
   * Initialize the channel (called by RPCClient)
   */
  public async init(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.initialized = true;
    const subject = `channel.${this.channelId}`;

    this.unsubscribe = await this.client.subscribe(subject, async (msg: ChannelMessage) => {
      switch (msg.type) {
        case 'message':
          this.emit('message', msg.data);
          break;
        case 'close':
          this.handleClose();
          break;
        case 'error':
          this.handleError(new Error(msg.error ?? 'Channel error'));
          break;
      }
    });
  }

  /**
   * Send data through the channel
   */
  public async send<TMessage = any>(data: TMessage): Promise<void> {
    if (this.closed) {
      throw new Error('Channel is closed');
    }

    const msg: ChannelMessage = {
      type: 'message',
      data,
    };

    await this.client.publish(`channel.${this.channelId}`, msg);
  }

  /**
   * Send a request and wait for reply
   */
  public async request<TRequest = any, TResponse = any>(data: TRequest, timeout = 5000): Promise<TResponse> {
    if (this.closed) {
      throw new Error('Channel is closed');
    }

    const subject = `channel.${this.channelId}.request`;
    return this.client.request<TRequest, TResponse>(subject, data, { timeout });
  }

  /**
   * Setup a request handler for this channel
   */
  public async onRequest<TRequest = any, TResponse = any>(handler: (data: TRequest) => Promise<TResponse> | TResponse): Promise<() => void> {
    const subject = `channel.${this.channelId}.request`;
    const unsubscribe = this.client.onRequest(subject, handler);
    this.subscriptions.push(unsubscribe);
    return unsubscribe;
  }

  /**
   * Listen for messages
   */
  public on<TEvent = any>(event: 'message', handler: (data: TEvent) => void): void;
  public on(event: 'close', handler: () => void): void;
  public on(event: 'error', handler: (error: Error) => void): void;
  public on(event: string, handler: any): void {
    if (event === 'close') {
      this.closeHandlers.add(handler);
    } else if (event === 'error') {
      this.errorHandlers.add(handler);
    } else {
      if (!this.handlers.has(event)) {
        this.handlers.set(event, new Set());
      }
      this.handlers.get(event)!.add(handler);
    }
  }

  /**
   * Remove event listener
   */
  public off<TEvent = any>(event: 'message', handler: (data: TEvent) => void): void;
  public off(event: 'close', handler: () => void): void;
  public off(event: 'error', handler: (error: Error) => void): void;
  public off(event: string, handler: any): void {
    if (event === 'close') {
      this.closeHandlers.delete(handler);
    } else if (event === 'error') {
      this.errorHandlers.delete(handler);
    } else {
      this.handlers.get(event)?.delete(handler);
    }
  }

  /**
   * Close the channel gracefully
   */
  public async close(): Promise<void> {
    if (this.closed) return;

    this.closed = true;

    // Try to notify other side
    try {
      const msg: ChannelMessage = { type: 'close' };
      await this.client.publish(`channel.${this.channelId}`, msg);
    } catch {
      // Ignore publish errors during close
    }

    // Cleanup
    await this.cleanup();
  }

  // Per-channel promise chain. Handlers for message / close / error are
  // serialized via this chain so dispatch stays ordered — matches Python's
  // channel.py:_emit/_handle_close/_handle_error behavior. The handler is
  // invoked INSIDE the chain; invoking eagerly and only chaining the
  // completion would run async handlers concurrently and out of order.
  private handlerChain: Promise<void> = Promise.resolve();

  private runHandler<Arg>(handler: (arg: Arg) => void, arg: Arg, label: string): void {
    this.handlerChain = this.handlerChain.then(() => handler(arg)).catch((error) => console.error(`Error in ${label}:`, error));
  }

  protected emit(event: string, data?: any): void {
    const handlers = this.handlers.get(event);
    if (!handlers) return;
    for (const handler of handlers) {
      this.runHandler(handler, data, 'channel handler');
    }
  }

  protected handleClose(): void {
    if (this.closed) return;

    this.closed = true;

    for (const handler of this.closeHandlers) {
      this.runHandler(() => handler(), undefined, 'close handler');
    }

    this.cleanup().catch(console.error);
  }

  protected handleError(error: Error): void {
    for (const handler of this.errorHandlers) {
      this.runHandler(handler, error, 'error handler');
    }
  }

  protected async cleanup(): Promise<void> {
    // Clear all handlers
    this.handlers.clear();
    this.closeHandlers.clear();
    this.errorHandlers.clear();

    // Clear subscriptions — each element is a Promise resolving to the
    // unsubscribe function; it must be awaited AND invoked.
    await Promise.allSettled(this.subscriptions.map(async (unsub) => (await unsub)()));
    this.subscriptions = [];

    // Unsubscribe from NATS
    if (this.unsubscribe) {
      try {
        this.unsubscribe();
      } catch {
        // Ignore unsubscribe errors
      }
      this.unsubscribe = undefined;
    }

    // Disconnect isolated client if present
    const isolatedClient: RPCClient | undefined = (this as any)._isolatedClient;
    if (isolatedClient) {
      try {
        await isolatedClient.disconnect();
      } catch {
        // Ignore disconnect errors
      }
    }
  }
}

/**
 * Private channel for 1:1 communication between two specific clients
 */
export class PrivateChannel extends Channel {
  private readonly clientId: string;
  private remoteClientId?: string;

  /**
   * Get the remote client ID (if connected)
   */
  get remoteId(): string | undefined {
    return this.remoteClientId;
  }

  constructor(
    client: RPCClient,
    channelId: string,
    private targetClientId: string,
  ) {
    super(client, channelId);
    // Use the original client name, not the isolated connection name
    // When using isolated connections, the name includes suffixes like '-channel-secret-chat'
    // We need to extract the original name to maintain consistent identity
    const fullName = client.options.name ?? `client-${Date.now()}`;
    // Extract base name by removing any suffixes added for isolated connections
    this.clientId = fullName.replace(/-(channel|private)-.*$/, '');
  }

  /**
   * Initialize private channel with handshake
   */
  public async init(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.initialized = true;

    // Use a unique subject that includes channelId and both client IDs for true privacy
    const sortedIds = [this.clientId, this.targetClientId].sort();
    const subject = `channel.private.${this.channelId}.${sortedIds.join('.')}`;

    this.unsubscribe = await this.client.subscribe(subject, async (msg: ChannelMessage) => {
      // Filter messages: only process if it's for us
      if (msg.sender === this.clientId) {
        // Skip our own messages
        return;
      }

      if (msg.sender !== this.targetClientId) {
        // Only accept messages from the target client
        return;
      }

      if (!this.remoteClientId && msg.sender) {
        // First message establishes the connection
        this.remoteClientId = msg.sender;
      }

      if (this.remoteClientId && msg.sender !== this.remoteClientId) {
        // After connection established, only accept from connected client
        return;
      }

      switch (msg.type) {
        case 'message':
          // Filter out handshake messages
          if (msg.data && typeof msg.data === 'object' && '__handshake' in msg.data) {
            // Handshake received, connection established
            return;
          }
          this.emit('message', msg.data);
          break;
        case 'close':
          this.handleClose();
          break;
        case 'error':
          this.handleError(new Error(msg.error ?? 'Channel error'));
          break;
      }
    });

    // Send initial handshake
    try {
      await this.sendRaw({
        type: 'message',
        data: { __handshake: true },
        sender: this.clientId,
      });
    } catch {
      // Ignore handshake errors - connection might still work
    }
  }

  /**
   * Send data through the private channel
   */
  public async send<TMessage = any>(data: TMessage): Promise<void> {
    if (this.closed) {
      throw new Error('Channel is closed');
    }

    await this.sendRaw({
      type: 'message',
      data,
      sender: this.clientId,
    });
  }

  /**
   * Send a request and wait for reply using native NATS request/reply
   */
  public async request<TRequest = any, TResponse = any>(data: TRequest, timeout = 5000): Promise<TResponse> {
    if (this.closed) {
      throw new Error('Channel is closed');
    }

    const sortedIds = [this.clientId, this.targetClientId].sort();
    const subject = `channel.private.${this.channelId}.${sortedIds.join('.')}.request`;

    return this.client.request(subject, data, { timeout });
  }

  /**
   * Setup a request handler for this private channel
   */
  public async onRequest<TRequest = any, TResponse = any>(handler: (data: TRequest) => Promise<TResponse> | TResponse): Promise<() => void> {
    const sortedIds = [this.clientId, this.targetClientId].sort();
    const subject = `channel.private.${this.channelId}.${sortedIds.join('.')}.request`;

    return this.client.onRequest(subject, handler);
  }

  /**
   * Close the private channel gracefully
   */
  public async close(): Promise<void> {
    if (this.closed) return;

    this.closed = true;

    // Try to notify remote client, but don't fail if it errors
    try {
      await this.sendRaw({
        type: 'close',
        sender: this.clientId,
      });
    } catch {
      // Ignore send errors during close
    }

    await this.cleanup();
  }

  /**
   * Check if channel is connected to a specific client
   */
  public isConnectedTo(clientId: string): boolean {
    return this.remoteClientId === clientId;
  }

  private async sendRaw(msg: ChannelMessage): Promise<void> {
    // Use the same subject format as in init()
    const sortedIds = [this.clientId, this.targetClientId].sort();
    const subject = `channel.private.${this.channelId}.${sortedIds.join('.')}`;
    await this.client.publish(subject, msg);
  }
}
