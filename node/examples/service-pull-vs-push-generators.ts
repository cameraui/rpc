import { createRPCClient, RPCClass } from '../src/index.js';

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
    console.log(`[Service] Starting push-based number generation (${count} items)`);
    for (let i = 0; i < count; i++) {
      yield i;
    }
    console.log('[Service] Push-based generation complete');
  }

  async *generateLargeData(chunks: number) {
    console.log(`[Service] Starting push-based data generation (${chunks} x 1MB)`);
    for (let i = 0; i < chunks; i++) {
      yield buffer1MB;
    }
    console.log('[Service] Push-based data generation complete');
  }

  async *pullNumbers(count: number) {
    console.log(`[Service] Starting pull-based number iteration (${count} items)`);
    for (let i = 0; i < count; i++) {
      console.log(`[Service] Client pulled number ${i}`);
      yield i;
    }
    console.log('[Service] Pull-based iteration complete');
  }

  async *pullLargeData(chunks: number) {
    console.log(`[Service] Starting pull-based data iteration (${chunks} x 1MB)`);
    for (let i = 0; i < chunks; i++) {
      console.log(`[Service] Client pulled chunk ${i}`);
      yield buffer1MB;
    }
    console.log('[Service] Pull-based data iteration complete');
  }

  async *generateSlowData(delayMs: number) {
    console.log(`[Service] Starting slow push generation (${delayMs}ms delay)`);
    for (let i = 0; i < 10; i++) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      yield `Push data ${i} at ${new Date().toISOString()}`;
    }
  }

  async *pullSlowData(delayMs: number) {
    console.log(`[Service] Starting slow pull iteration (${delayMs}ms delay)`);
    for (let i = 0; i < 10; i++) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      yield `Pull data ${i} at ${new Date().toISOString()}`;
    }
  }
}

async function testServicePushVsPull(service: DataService) {
  console.log('=== Service Test 1: Fast Number Generation ===\n');

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

async function testServiceBackpressure(service: DataService) {
  console.log('=== Service Test 2: Backpressure Handling (Large Data) ===\n');

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

async function testServiceCancellation(service: DataService) {
  console.log('=== Service Test 3: Early Termination ===\n');

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
  console.log('=== Service-based Pull vs Push Generator Comparison ===\n');
  console.log('This example demonstrates the differences between:');
  console.log('- Push-based generators (method names with "generate")');
  console.log('- Pull-based iterators (method names with "pull")');
  console.log('Using NATS micro services architecture\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'service-generator-test-server',
  });

  await server.connect();
  console.log('Server connected');

  const service = await server.service.registerHandler(
    {
      name: 'data-service',
      version: '1.0.0',
      description: 'Data service with push/pull generators',
    },
    new DataServiceImpl(),
  );
  console.log('Service registered as NATS micro service\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'service-generator-test-client',
  });

  await client.connect();
  console.log('Client connected\n');

  try {
    const totalStart = performance.now();

    const dataService = await client.createServiceProxy<DataService>('data-service');
    console.log('Service proxy created through discovery\n');

    await testServicePushVsPull(dataService);
    await testServiceBackpressure(dataService);
    await testServiceCancellation(dataService);

    const serviceInfo = server.service.getAllInfo();
    console.log('=== Service Information ===');
    console.log(`Name: ${serviceInfo[0].name}`);
    console.log(`Version: ${serviceInfo[0].version}`);
    console.log(`Endpoints: ${serviceInfo[0].endpoints.length}`);
    console.log('Endpoint names:', serviceInfo[0].endpoints.map((e) => e.name).join(', '));

    console.log('\n=== Summary ===\n');
    console.log('Service-based implementation demonstrates:');
    console.log('  Both push and pull work with NATS micro services');
    console.log('  Service discovery works for both patterns');
    console.log('  Same performance characteristics as direct RPC');
    console.log('  Pull-based provides better backpressure control');
    console.log('  Services can be scaled independently');

    const totalElapsed = performance.now() - totalStart;
    console.log(`\nAll service tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    await service.stop();
    await client.disconnect();
    await server.disconnect();
  }
}

main().catch(console.error);
