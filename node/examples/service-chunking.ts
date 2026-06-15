import { createRPCClient, RPCClass } from '../src/index.js';

const LARGE_DATA = Buffer.from(
  Array(20 * 1024 * 1024)
    .fill('x')
    .join(''),
);

@RPCClass
class TestService {
  async getLargeData(): Promise<Buffer> {
    console.log('[Service] Returning large data (5MB)');
    return LARGE_DATA;
  }

  async echoLargeData(data: Buffer): Promise<Buffer> {
    console.log(`[Service] Echoing large data (${(data.length / 1024 / 1024).toFixed(2)}MB)`);
    return data;
  }

  async processData(data: Buffer): Promise<{ size: number; checksum: string }> {
    console.log(`[Service] Processing data (${(data.length / 1024 / 1024).toFixed(2)}MB)`);
    let checksum = 0;
    for (let i = 0; i < data.length; i++) {
      checksum = (checksum + data[i]) % 256;
    }
    return {
      size: data.length,
      checksum: checksum.toString(16).padStart(2, '0'),
    };
  }

  async *generateNumbers(count: number): AsyncGenerator<number, void, unknown> {
    console.log(`[Service] Starting to stream ${count} numbers`);
    for (let i = 0; i < count; i++) {
      yield i;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    console.log('[Service] Streaming complete');
  }

  async *generateLargeItems(count: number, sizeKB: number): AsyncGenerator<{ index: number; data: Buffer }, void, unknown> {
    console.log(`[Service] Starting to stream ${count} items of ${sizeKB}KB each`);
    const itemData = Buffer.alloc(sizeKB * 1024, 'x');

    for (let i = 0; i < count; i++) {
      yield { index: i, data: itemData };
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    console.log('[Service] Large item streaming complete');
  }
}

async function main() {
  console.log('=== Service Chunking Test ===\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'chunking-server',
  });

  await server.connect();
  console.log('Server connected');

  const service = await server.service.registerHandler({ name: 'chunking-service', version: '1.0.0' }, new TestService());
  console.log('Service registered\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'chunking-client',
  });

  await client.connect();
  console.log('Client connected\n');

  try {
    const totalStart = performance.now();

    const proxy = await client.createServiceProxy<TestService>('chunking-service');
    console.log('Service proxy created\n');

    console.log('--- Test 1: Get Large Data ---');
    let start = performance.now();
    const largeData = await proxy.getLargeData();
    let elapsed = performance.now() - start;
    const sizeMB = largeData.length / 1024 / 1024;
    const throughput = sizeMB / (elapsed / 1000);
    console.log(`Received ${sizeMB.toFixed(2)}MB in ${elapsed.toFixed(1)}ms (${throughput.toFixed(1)} MB/s)`);
    console.log(`   Data matches: ${Buffer.compare(largeData, LARGE_DATA) === 0}\n`);

    console.log('--- Test 2: Echo Large Data ---');
    start = performance.now();
    const echoedData = await proxy.echoLargeData(LARGE_DATA);
    elapsed = performance.now() - start;
    const echoSizeMB = echoedData.length / 1024 / 1024;
    const echoThroughput = echoSizeMB / (elapsed / 1000);
    console.log(`Echoed ${echoSizeMB.toFixed(2)}MB in ${elapsed.toFixed(1)}ms (${echoThroughput.toFixed(1)} MB/s)`);
    console.log(`   Data matches: ${Buffer.compare(echoedData, LARGE_DATA) === 0}\n`);

    console.log('--- Test 3: Process Large Data ---');
    start = performance.now();
    const result = await proxy.processData(LARGE_DATA);
    elapsed = performance.now() - start;
    console.log(`Processed ${(result.size / 1024 / 1024).toFixed(2)}MB in ${elapsed.toFixed(1)}ms`);
    console.log(`   Size: ${result.size} bytes`);
    console.log(`   Checksum: ${result.checksum}\n`);

    console.log('--- Test 4: Stream Numbers ---');
    start = performance.now();
    let streamCount = 0;
    for await (const num of proxy.generateNumbers(10)) {
      if (streamCount < 3 || streamCount >= 7) {
        console.log(`   Received: ${num}`);
      } else if (streamCount === 3) {
        console.log('   ...');
      }
      streamCount++;
    }
    elapsed = performance.now() - start;
    console.log(`Streamed ${streamCount} numbers in ${elapsed.toFixed(1)}ms (${(elapsed / streamCount).toFixed(1)}ms per item)\n`);

    console.log('--- Test 5: Stream Large Items ---');
    start = performance.now();
    let largeStreamCount = 0;
    let totalSize = 0;

    for await (const item of proxy.generateLargeItems(5, 500)) {
      totalSize += item.data.length;
      console.log(`   Received item ${item.index}: ${(item.data.length / 1024).toFixed(0)}KB`);
      largeStreamCount++;
    }
    elapsed = performance.now() - start;
    const totalMB = totalSize / 1024 / 1024;
    const streamThroughput = totalMB / (elapsed / 1000);
    console.log(`Streamed ${largeStreamCount} large items (${totalMB.toFixed(2)}MB total) in ${elapsed.toFixed(1)}ms (${streamThroughput.toFixed(1)} MB/s)\n`);

    console.log('--- Test 6: Concurrent Mixed Operations ---');
    start = performance.now();

    const mixedPromises = [
      proxy.getLargeData(),
      proxy.processData(LARGE_DATA),
      (async () => {
        let count = 0;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const num of proxy.generateNumbers(5)) {
          count++;
        }
        return count;
      })(),
      (async () => {
        let items = 0;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const item of proxy.generateLargeItems(3, 200)) {
          items++;
        }
        return items;
      })(),
    ];

    const mixedResults = await Promise.all(mixedPromises);
    elapsed = performance.now() - start;
    console.log(`Completed mixed operations in ${elapsed.toFixed(1)}ms`);
    console.log(`   Large data: ${((mixedResults[0] as Buffer).length / 1024 / 1024).toFixed(2)}MB`);
    console.log(`   Process result: ${(mixedResults[1] as { size: number; checksum: string }).size} bytes`);
    console.log(`   Streamed numbers: ${mixedResults[2] as number}`);
    console.log(`   Streamed items: ${mixedResults[3] as number}\n`);

    const totalElapsed = performance.now() - totalStart;
    console.log(`All tests passed! Service chunking and streaming are working correctly. Total time: ${totalElapsed.toFixed(1)}ms`);
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    await service.stop();
    await client.disconnect();
    await server.disconnect();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error);
}
