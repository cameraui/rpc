import { createRPCClient, RPCClass, RPCMethod } from '../src/index.js';

@RPCClass
class GeneratorService {
  @RPCMethod
  async *asyncGeneratorGenerateNumbers(count: number): AsyncGenerator<number> {
    for (let i = 0; i < count; i++) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      yield i;
    }
  }

  @RPCMethod
  *syncGeneratorGenerateNumbers(count: number): Generator<number> {
    for (let i = 0; i < count; i++) {
      yield i * 2;
    }
  }

  @RPCMethod
  async asyncGenerateFuncReturningAsyncGen(count: number): Promise<AsyncGenerator<string>> {
    async function* innerGen(): AsyncGenerator<string> {
      for (let i = 0; i < count; i++) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        yield `async-${i}`;
      }
    }

    return innerGen();
  }

  @RPCMethod
  async asyncGenerateFuncReturningSyncGen(count: number): Promise<Generator<string>> {
    await new Promise((resolve) => setTimeout(resolve, 100));

    function* innerGen(): Generator<string> {
      for (let i = 0; i < count; i++) {
        yield `sync-from-async-${i}`;
      }
    }

    return innerGen();
  }

  @RPCMethod
  syncGenerateFuncReturningSyncGen(count: number): Generator<{ index: number; value: number }> {
    return (function* () {
      for (let i = 0; i < count; i++) {
        yield { index: i, value: i ** 2 };
      }
    })();
  }

  @RPCMethod
  async *mixedGenerateTypeGenerator(count: number): AsyncGenerator<number | string | { type: string; value: number }> {
    for (let i = 0; i < count; i++) {
      if (i % 3 === 0) {
        yield i;
      } else if (i % 3 === 1) {
        yield `string-${i}`;
      } else {
        yield { type: 'dict', value: i };
      }
    }
  }

  @RPCMethod
  getIterableArray(count: number): number[] {
    return Array.from({ length: count }, (_, i) => i * 3);
  }

  @RPCMethod
  async getAsyncIterableArray(count: number): Promise<string[]> {
    await new Promise((resolve) => setTimeout(resolve, 50));
    return Array.from({ length: count }, (_, i) => `item-${i}`);
  }
}

async function testGenerators() {
  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'generator-test-server',
    auth: { user: 'server', password: 'server_password' },
  });

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'generator-test-client',
    auth: { user: 'server', password: 'server_password' },
  });

  let unsubscribe: (() => Promise<void>) | undefined;

  try {
    await server.connect();
    await client.connect();

    const service = new GeneratorService();
    unsubscribe = await server.registerHandler('generator', service);

    const proxy = client.createProxy<GeneratorService>('generator');

    // Give services time to register
    await new Promise((resolve) => setTimeout(resolve, 100));

    console.log('Testing different generator types:\n');
    const totalStart = performance.now();

    console.log('1. Async generator function:');
    let start = performance.now();
    const values1: number[] = [];
    for await (const value of proxy.asyncGeneratorGenerateNumbers(5)) {
      values1.push(value);
      console.log(`   Received: ${value}`);
    }
    let elapsed = performance.now() - start;
    console.log(`   Time: ${elapsed.toFixed(1)}ms (5 items, ~${(elapsed / 5).toFixed(1)}ms per item)`);

    console.log('\n2. Sync generator function:');
    start = performance.now();
    const values2: number[] = [];
    for await (const value of proxy.syncGeneratorGenerateNumbers(5)) {
      values2.push(value);
      console.log(`   Received: ${value}`);
    }
    elapsed = performance.now() - start;
    console.log(`   Time: ${elapsed.toFixed(1)}ms (${values2.length} items, ~${(elapsed / values2.length).toFixed(1)}ms per item)`);

    console.log('\n3. Async function returning async generator:');
    start = performance.now();
    const values3: string[] = [];
    for await (const value of proxy.asyncGenerateFuncReturningAsyncGen(5)) {
      values3.push(value);
      console.log(`   Received: ${value}`);
    }
    elapsed = performance.now() - start;
    console.log(`   Time: ${elapsed.toFixed(1)}ms (${values3.length} items)`);

    console.log('\n4. Async function returning sync generator:');
    start = performance.now();
    const values4: string[] = [];
    for await (const value of proxy.asyncGenerateFuncReturningSyncGen(5)) {
      values4.push(value);
      console.log(`   Received: ${value}`);
    }
    elapsed = performance.now() - start;
    console.log(`   Time: ${elapsed.toFixed(1)}ms (${values4.length} items)`);

    console.log('\n5. Sync function returning sync generator:');
    start = performance.now();
    const values5: any[] = [];
    for await (const value of proxy.syncGenerateFuncReturningSyncGen(5)) {
      values5.push(value);
      console.log(`   Received: ${JSON.stringify(value)}`);
    }
    elapsed = performance.now() - start;
    console.log(`   Time: ${elapsed.toFixed(1)}ms (${values5.length} items)`);

    console.log('\n6. Mixed type generator:');
    start = performance.now();
    const typeCounts: Record<string, number> = { number: 0, string: 0, object: 0 };
    for await (const value of proxy.mixedGenerateTypeGenerator(9)) {
      const type = typeof value;
      typeCounts[type] = (typeCounts[type] || 0) + 1;
      console.log(`   Received: ${JSON.stringify(value)} (type: ${type})`);
    }
    elapsed = performance.now() - start;
    console.log(`   Time: ${elapsed.toFixed(1)}ms (types: ${JSON.stringify(typeCounts)})`);

    console.log('\n7. Function returning iterable (array):');
    start = performance.now();
    const iterableArray = await proxy.getIterableArray(5);
    elapsed = performance.now() - start;
    for (const value of iterableArray) {
      console.log(`   Received: ${value}`);
    }
    console.log(`   Time: ${elapsed.toFixed(1)}ms (returned ${iterableArray.length} items)`);

    console.log('\n8. Async function returning iterable:');
    start = performance.now();
    const asyncIterableArray = await proxy.getAsyncIterableArray(5);
    elapsed = performance.now() - start;
    for (const value of asyncIterableArray) {
      console.log(`   Received: ${value}`);
    }
    console.log(`   Time: ${elapsed.toFixed(1)}ms (returned ${asyncIterableArray.length} items)`);

    console.log('\n9. Early termination test:');
    start = performance.now();
    let count = 0;
    for await (const value of proxy.asyncGeneratorGenerateNumbers(100)) {
      console.log(`   Received: ${value}`);
      count++;
      if (count >= 3) {
        console.log('   Breaking early...');
        break;
      }
    }
    elapsed = performance.now() - start;
    console.log(`   Time: ${elapsed.toFixed(1)}ms (received ${count} items before breaking)`);

    const totalElapsed = performance.now() - totalStart;
    console.log(`\nAll generator tests passed. Total time: ${totalElapsed.toFixed(1)}ms`);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    if (unsubscribe) {
      await unsubscribe();
    }
    await client.disconnect();
    await server.disconnect();
  }
}

testGenerators().catch(console.error);
