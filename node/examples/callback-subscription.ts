import { createRPCClient } from '../src/index.js';

interface EventService {
  onEvents(prefix: string, callback: (event: any) => void | Promise<void>): Promise<() => void>;
}

class EventServiceImpl {
  private subscribers: ((event: any) => void | Promise<void>)[] = [];

  async onEvents(prefix: string, callback: (event: any) => void | Promise<void>): Promise<() => void> {
    this.subscribers.push(callback);
    console.log(`New subscriber for prefix '${prefix}'`);

    for (let i = 0; i < 5; i++) {
      await callback({ prefix, index: i, type: 'event' });
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    return () => {
      const idx = this.subscribers.indexOf(callback);
      if (idx >= 0) this.subscribers.splice(idx, 1);
      console.log(`Cleanup called for prefix '${prefix}'`);
    };
  }
}

async function main() {
  const totalStart = performance.now();

  console.log('Callback Subscription Example\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'callback-test-server',
  });
  await server.connect();

  const unsubHandler = await server.registerHandler('events', new EventServiceImpl(), { withoutDecorators: true });
  console.log('Server connected\nHandler registered\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'callback-test-client',
  });
  await client.connect();
  console.log('Client connected\n');

  const proxy = client.createProxy<EventService>('events');

  console.log('Test 1: Basic Callback Subscription\n');

  const received: any[] = [];
  let resolveDone: () => void;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  const start = performance.now();
  const unsubscribe = await proxy.onEvents('test', (event) => {
    received.push(event);
    console.log(`  Received event: prefix=${event.prefix} index=${event.index}`);
    if (received.length >= 5) resolveDone();
  });

  await Promise.race([done, new Promise((resolve) => setTimeout(resolve, 5000))]);
  const elapsed = performance.now() - start;

  console.log(`\n  Received ${received.length} events in ${elapsed.toFixed(1)}ms`);
  console.log('  Unsubscribing...');
  unsubscribe();
  console.log('  Unsubscribed!');

  console.log('\nTest 2: Multiple Concurrent Subscriptions\n');

  const counts = [0, 0, 0];
  const donePromises: Promise<void>[] = [];
  const unsubs: (() => void)[] = [];

  for (let i = 0; i < 3; i++) {
    const idx = i;
    let resolveSubDone: () => void;
    donePromises.push(
      new Promise((resolve) => {
        resolveSubDone = resolve;
      }),
    );

    const unsub = await proxy.onEvents(`sub-${i}`, () => {
      counts[idx]++;
      if (counts[idx] >= 5) resolveSubDone!();
    });
    unsubs.push(unsub);
  }

  await Promise.race([Promise.all(donePromises), new Promise((resolve) => setTimeout(resolve, 5000))]);

  for (const unsub of unsubs) {
    unsub();
  }

  for (let i = 0; i < 3; i++) {
    console.log(`  Subscriber ${i} received ${counts[i]} events`);
  }

  console.log('\nTest 3: Direct callWithCallback\n');

  let directCount = 0;
  let resolveDirectDone: () => void;
  const directDone = new Promise<void>((resolve) => {
    resolveDirectDone = resolve;
  });

  const directUnsub = await client.callWithCallback('rpc.events.onEvents', ['direct'], (event: any) => {
    directCount++;
    if (directCount <= 3) {
      console.log(`  Direct callback received: prefix=${event.prefix} index=${event.index}`);
    }
    if (directCount >= 5) resolveDirectDone();
  });

  await Promise.race([directDone, new Promise((resolve) => setTimeout(resolve, 5000))]);
  directUnsub();
  console.log(`  Total direct callbacks received: ${directCount}`);

  console.log('');
  if (received.length >= 5) {
    console.log('All assertions passed!');
  } else {
    console.log(`Expected at least 5 events, got ${received.length}`);
  }

  console.log('\nCleaning up...');
  await unsubHandler();
  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
