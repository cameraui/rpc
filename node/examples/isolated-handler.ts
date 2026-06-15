import { createRPCClient } from '../src/index.js';

async function main() {
  console.log('=== Isolated Handler Connection Test ===\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'main-server',
  });

  await server.connect();
  console.log('Main server connected');

  const handlers = {
    echo: async (msg: string) => {
      console.log(`Echo received: ${msg}`);
      return `Echo: ${msg}`;
    },

    getStats: async () => {
      console.log('Stats requested');
      return {
        timestamp: new Date().toISOString(),
        connections: 42,
        uptime: process.uptime(),
      };
    },

    slowOperation: async (delay: number) => {
      console.log(`Starting slow operation (${delay}ms)`);
      await new Promise((resolve) => setTimeout(resolve, delay));
      console.log('Slow operation completed');
      return `Completed after ${delay}ms`;
    },

    async *generateData(count: number) {
      console.log(`Streaming ${count} items`);
      for (let i = 0; i < count; i++) {
        yield { index: i, data: `Item ${i}` };
      }
      console.log('Stream completed');
    },
  };

  const unsubscribe = await server.registerHandler('isolated-test', handlers, { isolatedConnection: true });
  console.log('Handlers registered with isolated connection\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'test-client',
  });

  await client.connect();
  console.log('Client connected\n');

  const proxy = client.createProxy<{
    echo(msg: string): Promise<string>;
    getStats(): Promise<{ timestamp: string; connections: number; uptime: number }>;
    slowOperation(delay: number): Promise<string>;
    generateData(count: number): AsyncGenerator<{ index: number; data: string }>;
  }>('isolated-test');

  try {
    const totalStart = performance.now();

    console.log('Test 1: Echo');
    let start = performance.now();
    const echoResult = await proxy.echo('Hello Isolated World!');
    let elapsed = performance.now() - start;
    console.log(`Result: ${echoResult} - Time: ${elapsed.toFixed(1)}ms`);

    console.log('\nTest 2: Get Stats');
    start = performance.now();
    const stats = await proxy.getStats();
    elapsed = performance.now() - start;
    console.log(`Stats: ${JSON.stringify(stats)} - Time: ${elapsed.toFixed(1)}ms`);

    console.log('\nTest 3: Concurrent Calls');
    start = performance.now();
    const promises = [proxy.slowOperation(100), proxy.slowOperation(200), proxy.slowOperation(150), proxy.echo('Concurrent test'), proxy.getStats()];

    const results = await Promise.all(promises);
    elapsed = performance.now() - start;
    console.log(`All concurrent calls completed in ${elapsed.toFixed(1)}ms:`);
    results.forEach((result, i) => console.log(`  ${i + 1}: ${typeof result === 'object' ? JSON.stringify(result) : result}`));

    console.log('\nTest 4: Streaming');
    start = performance.now();
    const stream = proxy.generateData(5);
    let count = 0;
    for await (const item of stream) {
      console.log(`  Received: ${JSON.stringify(item)}`);
      count++;
    }
    elapsed = performance.now() - start;
    console.log(`Streamed ${count} items in ${elapsed.toFixed(1)}ms`);

    // Isolated handlers should also stop when the main server disconnects
    console.log('\nTest 5: Main Server Disconnect');
    console.log('Disconnecting main server...');
    await server.disconnect();
    console.log('Main server disconnected');

    console.log('Testing handlers after main disconnect...');
    try {
      await proxy.echo('Should fail');
      console.log('ERROR: Call should have failed');
    } catch (error) {
      console.log('Call failed as expected:', error instanceof Error ? error.message : error);
    }

    console.log('\nTest 6: Unsubscribe Handlers');
    await unsubscribe();
    console.log('Handlers unsubscribed');

    try {
      await proxy.echo('Should fail');
      console.log('ERROR: Call should have failed');
    } catch (error) {
      console.log('Call failed as expected:', error instanceof Error ? error.message : error);
    }

    const totalElapsed = performance.now() - totalStart;
    console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    await client.disconnect();
    if (server.isConnected) {
      await server.disconnect();
    }
  }
}

main().catch(console.error);
