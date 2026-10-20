import { generateId } from './utils.js';

import type { RPCClient } from './client.js';

export interface ChannelMessage {
  type: 'message' | 'close' | 'error';
  data?: any;
  error?: string;
  sender?: string;
}

export class Channel {
  protected closed = false;
  protected unsubscribe?: () => void;
  protected initialized = false;

  private readonly subject: string;
  private readonly requestSubject: string;

  private handlers = new Map<string, Set<(data: any) => void>>();
  private closeHandlers = new Set<() => void>();
  private errorHandlers = new Set<(error: Error) => void>();
  protected subscriptions: Promise<() => void>[] = [];
  protected readonly sender = generateId();

  get isClosed(): boolean {
    return this.closed;
  }

  get id(): string {
    return this.channelId;
  }

  constructor(
    protected client: RPCClient,
    protected channelId: string,
  ) {
    this.subject = `channel.${channelId}`;
    this.requestSubject = `channel.${channelId}.request`;
  }

  public async init(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.initialized = true;

    this.unsubscribe = await this.client.subscribe(this.subject, async (msg: ChannelMessage) => {
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

  public async send<TMessage = any>(data: TMessage): Promise<void> {
    if (this.closed) {
      throw new Error('Channel is closed');
    }

    const msg: ChannelMessage = {
      type: 'message',
      data,
    };

    await this.client.publish(this.subject, msg);
  }

  public async request<TRequest = any, TResponse = any>(data: TRequest, timeout = 5000): Promise<TResponse> {
    if (this.closed) {
      throw new Error('Channel is closed');
    }

    return this.client.request<TRequest, TResponse>(this.requestSubject, data, { timeout, sender: this.sender });
  }

  public async onRequest<TRequest = any, TResponse = any>(handler: (data: TRequest) => Promise<TResponse> | TResponse): Promise<() => void> {
    const unsubscribe = this.client.onRequest(this.requestSubject, handler, { ignoreSender: this.sender });
    this.subscriptions.push(unsubscribe);
    return unsubscribe;
  }

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

  public async close(): Promise<void> {
    if (this.closed) return;

    this.closed = true;

    try {
      const msg: ChannelMessage = { type: 'close' };
      await this.client.publish(this.subject, msg);
    } catch {
      // ignore
    }

    await this.cleanup();
  }

  private handlerChain: Promise<void> = Promise.resolve();

  private runHandler<Arg>(handler: (arg: Arg) => void, arg: Arg, label: string): void {
    // the handler runs inside the chain, chaining only its completion would reorder async handlers
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
    this.handlers.clear();
    this.closeHandlers.clear();
    this.errorHandlers.clear();

    // each entry is a promise of the unsubscribe function
    await Promise.allSettled(this.subscriptions.map(async (unsub) => (await unsub)()));
    this.subscriptions = [];

    if (this.unsubscribe) {
      try {
        this.unsubscribe();
      } catch {
        // ignore
      }
      this.unsubscribe = undefined;
    }

    const isolatedClient: RPCClient | undefined = (this as any)._isolatedClient;
    if (isolatedClient) {
      try {
        await isolatedClient.disconnect();
      } catch {
        // ignore
      }
      (this as any)._releaseIsolated?.();
    }
  }
}

export class PrivateChannel extends Channel {
  private readonly clientId: string;
  private remoteClientId?: string;

  private readonly privateSubject: string;
  private readonly privateRequestSubject: string;

  get remoteId(): string | undefined {
    return this.remoteClientId;
  }

  constructor(
    client: RPCClient,
    channelId: string,
    private targetClientId: string,
  ) {
    super(client, channelId);
    const fullName = client.options.name ?? `client-${Date.now()}`;
    // drop the suffix an isolated connection adds to the name, such as '-channel-secret-chat'
    this.clientId = fullName.replace(/-(channel|private)-.*$/, '');

    // sorted, so both ends build the same subject
    const sortedIds = [this.clientId, this.targetClientId].sort();
    this.privateSubject = `channel.private.${channelId}.${sortedIds.join('.')}`;
    this.privateRequestSubject = `${this.privateSubject}.request`;
  }

  public async init(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.initialized = true;

    this.unsubscribe = await this.client.subscribe(this.privateSubject, async (msg: ChannelMessage) => {
      if (msg.sender === this.clientId) {
        return;
      }

      if (msg.sender !== this.targetClientId) {
        return;
      }

      if (!this.remoteClientId && msg.sender) {
        this.remoteClientId = msg.sender;
      }

      if (this.remoteClientId && msg.sender !== this.remoteClientId) {
        return;
      }

      switch (msg.type) {
        case 'message':
          if (msg.data && typeof msg.data === 'object' && '__handshake' in msg.data) {
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

    try {
      await this.sendRaw({
        type: 'message',
        data: { __handshake: true },
        sender: this.clientId,
      });
    } catch {
      // the channel may still work
    }
  }

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

  public async request<TRequest = any, TResponse = any>(data: TRequest, timeout = 5000): Promise<TResponse> {
    if (this.closed) {
      throw new Error('Channel is closed');
    }

    return this.client.request(this.privateRequestSubject, data, { timeout, sender: this.sender });
  }

  public async onRequest<TRequest = any, TResponse = any>(handler: (data: TRequest) => Promise<TResponse> | TResponse): Promise<() => void> {
    const unsubscribe = this.client.onRequest(this.privateRequestSubject, handler, { ignoreSender: this.sender });
    this.subscriptions.push(unsubscribe);
    return unsubscribe;
  }

  public async close(): Promise<void> {
    if (this.closed) return;

    this.closed = true;

    try {
      await this.sendRaw({
        type: 'close',
        sender: this.clientId,
      });
    } catch {
      // ignore
    }

    await this.cleanup();
  }

  public isConnectedTo(clientId: string): boolean {
    return this.remoteClientId === clientId;
  }

  private async sendRaw(msg: ChannelMessage): Promise<void> {
    await this.client.publish(this.privateSubject, msg);
  }
}
