import { createRPCClient } from '../src/index.js';

const handlers = {
  name: 'test-service',
  version: '1.0.0',

  getName: async () => {
    return handlers.name;
  },

  getVersion: async () => {
    return handlers.version;
  },

  echo: async (msg: string) => {
    return `Echo: ${msg}`;
  },
};

async function main() {
  console.log('Plain Object Property Access Test (TypeScript)\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'plain-object-server',
  });
  await server.connect();
  console.log('Server connected');

  const unsub = await server.registerHandler('test', handlers);
  console.log('Handlers registered\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'plain-object-client',
  });
  await client.connect();
  console.log('Client connected\n');

  const proxy = client.createProxy<{
    name: Promise<string>;
    version: Promise<string>;
    getName(): Promise<string>;
    getVersion(): Promise<string>;
    echo(msg: string): Promise<string>;
  }>('test');

  try {
    const totalStart = performance.now();

    console.log('Testing Direct Property Access');

    let start = performance.now();
    const name = await proxy.name;
    let elapsed = performance.now() - start;
    console.log(`Name (direct): ${name} - Time: ${elapsed.toFixed(1)}ms`);

    start = performance.now();
    const version = await proxy.version;
    elapsed = performance.now() - start;
    console.log(`Version (direct): ${version} - Time: ${elapsed.toFixed(1)}ms`);

    console.log('\nTesting Method Calls');
    start = performance.now();
    const nameViaMethod = await proxy.getName();
    elapsed = performance.now() - start;
    console.log(`Name (via method): ${nameViaMethod} - Time: ${elapsed.toFixed(1)}ms`);

    start = performance.now();
    const echo = await proxy.echo('Hello World');
    elapsed = performance.now() - start;
    console.log(`Echo: ${echo} - Time: ${elapsed.toFixed(1)}ms`);

    console.log('\nPerformance Test');
    const iterations = 100;

    start = performance.now();
    for (let i = 0; i < iterations; i++) {
      await proxy.name;
    }
    elapsed = performance.now() - start;
    console.log(`Direct property access: ${iterations} calls in ${elapsed.toFixed(1)}ms (${(elapsed / iterations).toFixed(2)}ms avg)`);

    start = performance.now();
    for (let i = 0; i < iterations; i++) {
      await proxy.echo('test');
    }
    elapsed = performance.now() - start;
    console.log(`Method calls: ${iterations} calls in ${elapsed.toFixed(1)}ms (${(elapsed / iterations).toFixed(2)}ms avg)`);

    const totalElapsed = performance.now() - totalStart;
    console.log(`\nAll tests passed! Total time: ${totalElapsed.toFixed(1)}ms`);
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    console.log('\nCleaning up...');
    await unsub();
    await client.disconnect();
    await server.disconnect();
  }
}

main().catch(console.error);
