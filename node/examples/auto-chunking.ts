import { RPCClass } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

const smallData = 'This is a small response that fits in a single message';
const mediumData = Buffer.alloc(5 * 1024 * 1024); // 5MB
const largeData = Buffer.alloc(50 * 1024 * 1024); // 50MB

for (let i = 0; i < mediumData.length; i++) {
  mediumData[i] = i % 256;
}

for (let i = 0; i < largeData.length; i++) {
  largeData[i] = i % 256;
}

const chunkBuffers: Buffer[] = [];
for (let i = 0; i < 3; i++) {
  const buffer = Buffer.alloc(5 * 1024 * 1024);
  for (let j = 0; j < buffer.length; j++) {
    buffer[j] = (i + j) % 256;
  }
  chunkBuffers.push(buffer);
}

const echoData = Buffer.alloc(4 * 1024 * 1024);
for (let i = 0; i < echoData.length; i++) {
  echoData[i] = (i * 2) % 256;
}

interface TestService {
  getSmallData(): Promise<string>;
  getMediumData(): Promise<Buffer>;
  getLargeData(): Promise<Buffer>;

  echo(data: Buffer): Promise<Buffer>;

  generateLargeDataStream(): AsyncGenerator<Buffer>;

  ping(): Promise<string>;
}

@RPCClass
class TestServiceImpl implements TestService {
  async getSmallData() {
    return smallData;
  }

  async getMediumData() {
    console.log('Returning 5MB buffer');
    return mediumData;
  }

  async getLargeData() {
    console.log('Returning 50MB buffer');
    return largeData;
  }

  async echo(data: Buffer) {
    console.log(`Echoing ${(data.length / 1024 / 1024).toFixed(2)}MB`);
    return data;
  }

  async *generateLargeDataStream() {
    console.log('Streaming 3 chunks of 5MB each');

    for (let i = 0; i < 3; i++) {
      console.log(`Yielding chunk ${i + 1}/3 - size: ${chunkBuffers[i].length} bytes`);
      console.log(`First bytes: [${Array.from(chunkBuffers[i].slice(0, 10)).join(', ')}]`);
      yield chunkBuffers[i];

      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  async ping() {
    return `pong at ${new Date().toISOString()}`;
  }
}

async function main() {
  const totalStart = performance.now();

  console.log('Automatic Chunking Test\n');
  console.log('Testing transparent chunking in RPC library');
  console.log('Note: NATS server default max_payload is typically 1MB');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'test-client',
    auth: { user: 'server', password: 'server_password' },
  });

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'test-server',
    auth: { user: 'server', password: 'server_password' },
  });

  await client.connect();
  await server.connect();

  const unsub1 = await server.registerHandler('test', new TestServiceImpl());
  const unsub2 = await server.registerHandler('testSmall', new TestServiceImpl());

  const service = client.createProxy<TestService>('test');

  console.log('\n--- Test 1: Small Data ---');
  const smallStart = performance.now();
  const smallResult = await service.getSmallData();
  console.log(`Small data received in ${(performance.now() - smallStart).toFixed(0)}ms: "${smallResult}"`);

  console.log('\n--- Test 2: Medium Data (5MB) ---');
  const mediumStart = performance.now();
  const mediumDataResult = await service.getMediumData();
  const mediumTime = performance.now() - mediumStart;
  console.log(`Medium data received: ${(mediumDataResult.length / 1024 / 1024).toFixed(2)}MB in ${mediumTime.toFixed(0)}ms`);
  console.log(`Throughput: ${(5 / (mediumTime / 1000)).toFixed(2)} MB/s`);

  let corrupted = false;
  for (let i = 0; i < Math.min(1000, mediumDataResult.length); i++) {
    if (mediumDataResult[i] !== i % 256) {
      corrupted = true;
      break;
    }
  }
  console.log(`Data integrity: ${corrupted ? 'CORRUPTED' : 'OK'}`);

  console.log('\n--- Test 3: Large Data (50MB) ---');
  const largeStart = performance.now();
  const largeDataResult = await service.getLargeData();
  const largeTime = performance.now() - largeStart;
  console.log(`Large data received: ${(largeDataResult.length / 1024 / 1024).toFixed(2)}MB in ${largeTime.toFixed(0)}ms`);
  console.log(`Throughput: ${(50 / (largeTime / 1000)).toFixed(2)} MB/s`);

  console.log('\n--- Test 4: Echo Test (4MB round-trip) ---');
  const echoStart = performance.now();
  const echoed = await service.echo(echoData);
  const echoTime = performance.now() - echoStart;
  console.log(`Echo completed in ${echoTime.toFixed(0)}ms`);
  console.log(`Round-trip throughput: ${(8 / (echoTime / 1000)).toFixed(2)} MB/s`);
  console.log(`Data matches: ${Buffer.compare(echoData, echoed) === 0 ? 'yes' : 'no'}`);

  console.log('\n--- Test 5: Streaming Large Chunks ---');
  const streamStart = performance.now();
  let streamedBytes = 0;
  let chunkCount = 0;

  for await (const chunk of service.generateLargeDataStream()) {
    chunkCount++;
    streamedBytes += chunk.length;
    console.log(`Received stream chunk ${chunkCount}: ${(chunk.length / 1024 / 1024).toFixed(2)}MB`);
  }

  const streamTime = performance.now() - streamStart;
  console.log(`Stream completed: ${(streamedBytes / 1024 / 1024).toFixed(2)}MB in ${streamTime.toFixed(0)}ms`);
  console.log(`Stream throughput: ${(streamedBytes / 1024 / 1024 / (streamTime / 1000)).toFixed(2)} MB/s`);

  console.log('\n--- Test 6: Concurrent Operations ---');
  const concurrentTasks: Promise<void>[] = [];

  const largeTransfer = async () => {
    const data = await service.getLargeData();
    console.log(`Large data received: ${(data.length / 1024 / 1024).toFixed(2)}MB`);
  };

  concurrentTasks.push(largeTransfer());

  for (let i = 0; i < 5; i++) {
    const pingTest = async (index: number) => {
      const start = performance.now();
      await service.ping();
      console.log(`Ping ${index + 1} latency: ${(performance.now() - start).toFixed(0)}ms`);
    };
    concurrentTasks.push(pingTest(i));

    await new Promise((resolve) => setTimeout(resolve, i * 100));
  }

  await Promise.all(concurrentTasks);

  console.log('\n--- Test 7: Extreme Chunking (1KB max payload) ---');

  const clientSmall = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'test-client-small',
    auth: { user: 'server', password: 'server_password' },
  });

  await clientSmall.connect();

  // Override max payload size to test extreme chunking
  // @ts-ignore - accessing private property for testing
  clientSmall._maxPayloadSize = 1024; // 1KB

  const serviceSmall = clientSmall.createProxy<TestService>('testSmall');

  const smallDataResult = await serviceSmall.getSmallData();
  console.log(`Small data with 1KB limit: "${smallDataResult}"`);

  const tenKB = await serviceSmall.echo(Buffer.alloc(10 * 1024, 0x42));
  console.log(`10KB data with 1KB limit: received ${tenKB.length} bytes in ${(performance.now() - streamStart).toFixed(0)}ms (~10 chunks)`);

  const fiveMBStart = performance.now();
  const fiveMBData = await serviceSmall.getMediumData();
  console.log(`5MB data with 1KB limit: received ${fiveMBData.length} bytes in ${(performance.now() - fiveMBStart).toFixed(0)}ms (~5120 chunks)`);

  await clientSmall.disconnect();

  await unsub1();
  await unsub2();
  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
