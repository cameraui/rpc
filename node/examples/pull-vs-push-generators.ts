import { RPCClass } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

const buffer1MB = Buffer.alloc(1 * 1024 * 1024);
for (let i = 0; i < buffer1MB.length; i++) {
  buffer1MB[i] = i % 256;
}

interface DataService {
  generateNumbers(count: number): AsyncGenerator<number>;
  generateLargeData(chunks: number): AsyncGenerator<Buffer>;

  pullNumbers(count: number): AsyncGenerator<number>;
  pullLargeData(chunks: number): AsyncGenerator<Buffer>;

  generateSlowData(delayMs: number): AsyncGenerator<string>;
  pullSlowData(delayMs: number): AsyncGenerator<string>;
}

@RPCClass
class DataServiceImpl implements DataService {
  async *generateNumbers(count: number) {
    console.log(`[Server] Starting push-based number generation (${count} items)`);
    for (let i = 0; i < count; i++) {
      yield i;
    }
    console.log('[Server] Push-based generation complete');
  }

  async *generateLargeData(chunks: number) {
    console.log(`[Server] Starting push-based data generation (${chunks} x 1MB)`);
    for (let i = 0; i < chunks; i++) {
      yield buffer1MB;
    }
    console.log('[Server] Push-based data generation complete');
  }

  async *pullNumbers(count: number) {
    console.log(`[Server] Starting pull-based number iteration (${count} items)`);
    for (let i = 0; i < count; i++) {
      console.log(`[Server] Client pulled number ${i}`);
      yield i;
    }
    console.log('[Server] Pull-based iteration complete');
  }

  async *pullLargeData(chunks: number) {
    console.log(`[Server] Starting pull-based data iteration (${chunks} x 1MB)`);
    for (let i = 0; i < chunks; i++) {
      console.log(`[Server] Client pulled chunk ${i}`);
      yield buffer1MB;
    }
    console.log('[Server] Pull-based data iteration complete');
  }

  async *generateSlowData(delayMs: number) {
    console.log(`[Server] Starting slow push generation (${delayMs}ms delay)`);
    for (let i = 0; i < 10; i++) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      yield `Push data ${i} at ${new Date().toISOString()}`;
    }
  }

  async *pullSlowData(delayMs: number) {
    console.log(`[Server] Starting slow pull iteration (${delayMs}ms delay)`);
    for (let i = 0; i < 10; i++) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      yield `Pull data ${i} at ${new Date().toISOString()}`;
    }
  }
}

async function testPushVsPull(service: DataService) {
  console.log('=== Test 1: Fast Number Generation ===\n');

  let start = performance.now();
  let count = 0;
  console.log('[Client] Starting push-based consumption...');
  for await (const num of service.generateNumbers(1000)) {
    count++;
    if (count % 100 === 0) {
      console.log(`[Client] Processing pushed number ${num}...`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  const pushTime = performance.now() - start;
  console.log(`[Client] Push-based: Received ${count} numbers in ${pushTime.toFixed(1)}ms\n`);

  start = performance.now();
  count = 0;
  console.log('[Client] Starting pull-based consumption...');
  for await (const num of service.pullNumbers(1000)) {
    count++;
    if (count % 100 === 0) {
      console.log(`[Client] Processing pulled number ${num}...`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  const pullTime = performance.now() - start;
  console.log(`[Client] Pull-based: Received ${count} numbers in ${pullTime.toFixed(1)}ms\n`);

  console.log(`Performance comparison: Push ${pushTime.toFixed(1)}ms vs Pull ${pullTime.toFixed(1)}ms\n`);
}

async function testBackpressure(service: DataService) {
  console.log('=== Test 2: Backpressure Handling (Large Data) ===\n');

  console.log('[Client] Testing push-based with slow consumer...');
  let start = performance.now();
  let bytesReceived = 0;
  let chunkCount = 0;

  for await (const chunk of service.generateLargeData(10)) {
    bytesReceived += chunk.length;
    chunkCount++;
    console.log(`[Client] Received push chunk ${chunkCount}, total: ${(bytesReceived / 1024 / 1024).toFixed(1)}MB`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  const pushTime = performance.now() - start;
  const pushThroughput = bytesReceived / 1024 / 1024 / (pushTime / 1000);
  console.log(`[Client] Push completed: ${(bytesReceived / 1024 / 1024).toFixed(1)}MB in ${pushTime.toFixed(1)}ms (${pushThroughput.toFixed(1)} MB/s)\n`);

  console.log('[Client] Testing pull-based with slow consumer...');
  start = performance.now();
  bytesReceived = 0;
  chunkCount = 0;

  for await (const chunk of service.pullLargeData(10)) {
    bytesReceived += chunk.length;
    chunkCount++;
    console.log(`[Client] Received pull chunk ${chunkCount}, total: ${(bytesReceived / 1024 / 1024).toFixed(1)}MB`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  const pullTime = performance.now() - start;
  const pullThroughput = bytesReceived / 1024 / 1024 / (pullTime / 1000);
  console.log(`[Client] Pull completed: ${(bytesReceived / 1024 / 1024).toFixed(1)}MB in ${pullTime.toFixed(1)}ms (${pullThroughput.toFixed(1)} MB/s)\n`);
}

async function testMemoryPressure(service: DataService) {
  console.log('=== Test 3: Memory Pressure Simulation ===\n');

  const initialMemory = process.memoryUsage();
  console.log(`Initial memory: ${(initialMemory.heapUsed / 1024 / 1024).toFixed(1)}MB`);

  console.log('\n[Client] Starting push-based with intentional delay...');
  let maxMemoryPush = initialMemory.heapUsed;

  const pushIterator = service.generateLargeData(20);
  await new Promise((resolve) => setTimeout(resolve, 1000));

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const chunk of pushIterator) {
    const currentMemory = process.memoryUsage().heapUsed;
    maxMemoryPush = Math.max(maxMemoryPush, currentMemory);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }

  console.log(`Push-based peak memory: ${(maxMemoryPush / 1024 / 1024).toFixed(1)}MB`);

  console.log('\n[Client] Starting pull-based with same delay pattern...');
  let maxMemoryPull = initialMemory.heapUsed;

  const pullIterator = service.pullLargeData(20);
  // Delay has no effect - nothing is generated yet
  await new Promise((resolve) => setTimeout(resolve, 1000));

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const chunk of pullIterator) {
    const currentMemory = process.memoryUsage().heapUsed;
    maxMemoryPull = Math.max(maxMemoryPull, currentMemory);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }

  console.log(`Pull-based peak memory: ${(maxMemoryPull / 1024 / 1024).toFixed(1)}MB`);
  console.log(`Memory difference: ${((maxMemoryPush - maxMemoryPull) / 1024 / 1024).toFixed(1)}MB\n`);
}

async function testCancellation(service: DataService) {
  console.log('=== Test 4: Early Termination ===\n');

  console.log('[Client] Testing push-based early termination...');
  let start = performance.now();
  let count = 0;

  for await (const data of service.generateSlowData(100)) {
    console.log(`[Client] Received push: ${data}`);
    count++;
    if (count >= 3) {
      console.log('[Client] Breaking from push loop...');
      break;
    }
  }
  const pushTime = performance.now() - start;
  console.log(`[Client] Push terminated after ${count} items in ${pushTime.toFixed(1)}ms\n`);

  console.log('[Client] Testing pull-based early termination...');
  start = performance.now();
  count = 0;

  for await (const data of service.pullSlowData(100)) {
    console.log(`[Client] Received pull: ${data}`);
    count++;
    if (count >= 3) {
      console.log('[Client] Breaking from pull loop...');
      break;
    }
  }
  const pullTime = performance.now() - start;
  console.log(`[Client] Pull terminated after ${count} items in ${pullTime.toFixed(1)}ms\n`);
}

async function main() {
  console.log('=== Pull vs Push Generator Comparison ===\n');
  console.log('This example demonstrates the differences between:');
  console.log('- Push-based generators (method names with "generate")');
  console.log('- Pull-based iterators (method names with "pull")\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'generator-test-server',
  });

  await server.connect();
  console.log('Server connected');

  const unsub = await server.registerHandler('data', new DataServiceImpl());
  console.log('Service registered\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'generator-test-client',
  });

  await client.connect();
  console.log('Client connected\n');

  try {
    const totalStart = performance.now();

    const service = client.createProxy<DataService>('data');

    await testPushVsPull(service);
    await testBackpressure(service);
    await testMemoryPressure(service);
    await testCancellation(service);

    console.log('=== Summary ===\n');
    console.log('Push-based (generate*):');
    console.log('  Server sends all data immediately');
    console.log('  Good for small datasets or fast consumers');
    console.log('  Can cause memory pressure with slow consumers');
    console.log('  No natural backpressure');
    console.log();
    console.log('Pull-based (pull*):');
    console.log('  Client controls the flow');
    console.log('  Natural backpressure handling');
    console.log('  Memory efficient');
    console.log('  Better cancellation behavior');
    console.log('  Slightly more latency per item');

    const totalElapsed = performance.now() - totalStart;
    console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    await unsub();
    await client.disconnect();
    await server.disconnect();
  }
}

main().catch(console.error);
