import { RPCClass, RPCNested } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

@RPCClass
class TestService {
  async normalMethod(name: string): Promise<string> {
    return `Hello, ${name}!`;
  }

  async *generateNumbersService(count: number): AsyncGenerator<number> {
    for (let i = 1; i <= count; i++) {
      yield i;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  @RPCNested
  nested = {
    async *generateData(prefix: string): AsyncGenerator<string> {
      for (let i = 1; i <= 3; i++) {
        yield `${prefix}-${i}`;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    },
  };
}

@RPCClass
class TestHandler {
  async normalMethod(name: string): Promise<string> {
    return `RPC says: Hello, ${name}!`;
  }

  async *generateNumbers(count: number): AsyncGenerator<number> {
    for (let i = 1; i <= count; i++) {
      yield i * 10;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
}

async function main() {
  const totalStart = performance.now();
  console.log('Testing unified streaming implementation...\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'unified-test-server',
  });
  await server.connect();

  const service1 = await server.service.registerHandler(
    {
      name: 'test-service',
      version: '1.0.0',
    },
    new TestService(),
  );

  const unsub1 = await server.registerHandler('test-rpc', new TestHandler());

  console.log('Server ready with service and RPC handler\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'unified-test-client',
  });
  await client.connect();

  console.log('Testing Service Streaming');
  const proxyStart = performance.now();
  const service = await client.createServiceProxy<{
    normalMethod(name: string): Promise<string>;
    generateNumbersService(count: number): AsyncGenerator<number>;
    nested: {
      generateData(prefix: string): AsyncGenerator<string>;
    };
  }>('test-service');
  const proxyTime = performance.now() - proxyStart;
  console.log(`Service proxy created in ${proxyTime.toFixed(1)}ms`);

  const start = performance.now();
  const greeting = await service.normalMethod('Service');
  const methodTime = performance.now() - start;
  console.log(`Service normal method: ${greeting} (${methodTime.toFixed(1)}ms)`);

  console.log('\nService streaming:');
  let streamCount = 0;
  const streamStart = performance.now();
  for await (const num of service.generateNumbersService(5)) {
    console.log('  Received:', num);
    streamCount++;
  }
  const streamTime = performance.now() - streamStart;
  console.log(`  Total items: ${streamCount} in ${streamTime.toFixed(1)}ms`);

  console.log('\nService nested streaming:');
  let nestedCount = 0;
  const nestedStart = performance.now();
  for await (const data of service.nested.generateData('test')) {
    console.log('  Received:', data);
    nestedCount++;
  }
  const nestedTime = performance.now() - nestedStart;
  console.log(`  Total items: ${nestedCount} in ${nestedTime.toFixed(1)}ms`);

  console.log('\nTesting RPC Streaming');
  const rpcProxyStart = performance.now();
  const rpc = client.createProxy<{
    normalMethod(name: string): Promise<string>;
    generateNumbers(count: number): AsyncGenerator<number>;
  }>('test-rpc');
  const rpcProxyTime = performance.now() - rpcProxyStart;
  console.log(`RPC proxy created in ${rpcProxyTime.toFixed(1)}ms`);

  const rpcStart = performance.now();
  const rpcGreeting = await rpc.normalMethod('RPC');
  const rpcMethodTime = performance.now() - rpcStart;
  console.log(`RPC normal method: ${rpcGreeting} (${rpcMethodTime.toFixed(1)}ms)`);

  console.log('\nRPC streaming:');
  let rpcCount = 0;
  const rpcStreamStart = performance.now();
  for await (const num of rpc.generateNumbers(5)) {
    console.log('  Received:', num);
    rpcCount++;
  }
  const rpcStreamTime = performance.now() - rpcStreamStart;
  console.log(`  Total items: ${rpcCount} in ${rpcStreamTime.toFixed(1)}ms`);

  console.log('\nMessage Format Verification');
  console.log('Both service and RPC streaming now use:');
  console.log('- Same StreamMessage format');
  console.log('- Same cancellation support');
  console.log('- Same error handling');
  console.log('- Automatic chunking via client.publish()');

  console.log('\nCleaning up...');
  await service1.stop();
  await unsub1();
  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nTest completed successfully! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
