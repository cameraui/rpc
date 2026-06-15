import { RPCClass } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

interface TestService {
  ping(): Promise<string>;
  heavyComputation(seconds: number): Promise<number>;
  generateDataStream(): AsyncGenerator<string>;
}

@RPCClass
class TestServiceImpl implements TestService {
  async ping() {
    return `pong at ${new Date().toISOString()}`;
  }

  async heavyComputation(seconds: number) {
    console.log(`Starting heavy computation for ${seconds}s`);
    const start = performance.now();
    while (performance.now() - start < seconds * 1000) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    console.log('Heavy computation completed');
    return performance.now() - start;
  }

  async *generateDataStream() {
    for (let i = 0; i < 100; i++) {
      yield `Data chunk ${i} at ${new Date().toISOString()}`;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
}

async function testIsolatedChannels() {
  console.log('=== Testing Isolated Channels ===\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'test-client',
  });

  await client.connect();

  const regularChannel = await client.channel('regular-channel');

  const isolatedChannel = await client.channel('isolated-channel', {
    isolatedConnection: true,
  });

  regularChannel.on('message', (data) => {
    if (data.file) {
      data.file = `<${data.file.length} bytes>`;
    }

    console.log('Regular Channel:', data);
  });

  isolatedChannel.on('message', (data) => {
    if (data.file) {
      data.file = `<${data.file.length} bytes>`;
    }

    console.log('Isolated Channel:', data);
  });

  console.log('Sending heavy traffic on isolated channel...');
  const isolatedStart = performance.now();
  const promises: Promise<void>[] = [];
  const testBuffer = Buffer.alloc(10000).fill('x');
  for (let i = 0; i < 1000; i++) {
    promises.push(
      isolatedChannel.send({
        type: 'bulk',
        index: i,
        data: testBuffer,
      }),
    );
  }

  console.log('Testing regular channel responsiveness...');
  const regularStart = performance.now();
  await regularChannel.send({ type: 'ping', timestamp: performance.now() });
  const regularTime = performance.now() - regularStart;
  console.log(`Regular channel responded in ${regularTime.toFixed(1)}ms`);

  await Promise.all(promises);
  const isolatedTime = performance.now() - isolatedStart;
  const throughput = (1000 * 10) / (isolatedTime / 1000); // 10KB * 1000 messages
  console.log(`Heavy traffic completed in ${isolatedTime.toFixed(1)}ms (${(throughput / 1024).toFixed(1)} MB/s)\n`);

  await regularChannel.close();
  await isolatedChannel.close();
  await client.disconnect();
}

async function testIsolatedProxies() {
  console.log('=== Testing Isolated Proxies ===\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'test-server',
  });

  await server.connect();
  const unsub = await server.registerHandler('test', new TestServiceImpl());

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'test-client',
  });

  await client.connect();

  const regularProxy = client.createProxy<TestService>('test');

  const isolatedProxy = client.createProxy<TestService>('test', {
    isolatedConnection: true,
  });

  console.log('Starting heavy computation on isolated proxy...');
  const heavyStart = performance.now();
  const heavyPromise = isolatedProxy.proxy.heavyComputation(3);

  console.log('Testing regular proxy responsiveness...');
  const pingTimes: number[] = [];
  for (let i = 0; i < 5; i++) {
    const pingStart = performance.now();
    const result = await regularProxy.ping();
    const pingTime = performance.now() - pingStart;
    pingTimes.push(pingTime);
    console.log(`Regular proxy ping ${i + 1}: ${pingTime.toFixed(1)}ms - ${result}`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  const avgPingTime = pingTimes.reduce((a, b) => a + b, 0) / pingTimes.length;
  console.log(`Average ping time during heavy computation: ${avgPingTime.toFixed(1)}ms`);

  const computationTime = await heavyPromise;
  const totalHeavyTime = performance.now() - heavyStart;
  console.log(`Heavy computation completed in ${computationTime}ms (total: ${totalHeavyTime.toFixed(1)}ms)\n`);

  console.log('Testing streaming on isolated proxy...');
  const streamStart = performance.now();
  let count = 0;
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const data of isolatedProxy.proxy.generateDataStream()) {
    count++;
    if (count % 20 === 0) {
      console.log(`Received ${count} chunks from isolated stream`);
    }
  }
  const streamTime = performance.now() - streamStart;
  console.log(`Stream completed: ${count} total chunks in ${streamTime.toFixed(1)}ms (${((count * 1000) / streamTime).toFixed(1)} chunks/s)\n`);

  await unsub();

  await isolatedProxy.close();
  await client.disconnect();
  await server.disconnect();

  console.log('Isolated proxies test completed\n');
}

async function testMixedWorkload() {
  console.log('=== Testing Mixed Workload ===\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'mixed-client',
  });

  await client.connect();

  const chatChannel = await client.privateChannel('chat', 'partner', {
    isolatedConnection: true,
  });

  const dataChannel = await client.privateChannel('data-transfer', 'partner', {
    isolatedConnection: true,
  });

  console.log('Starting mixed workload...');

  const chatInterval = setInterval(async () => {
    await chatChannel.send({
      type: 'chat',
      message: 'Hello from chat',
      timestamp: new Date().toISOString(),
    });
  }, 1000);

  const dataStart = performance.now();
  const dataPromises: Promise<void>[] = [];
  const dataBuffer = Buffer.alloc(100 * 1024).fill('x');
  for (let i = 0; i < 100; i++) {
    dataPromises.push(
      dataChannel.send({
        type: 'data',
        chunk: i,
        payload: dataBuffer,
      }),
    );
  }

  let chatMessages = 0;
  let dataChunks = 0;

  chatChannel.on('message', () => chatMessages++);
  dataChannel.on('message', () => dataChunks++);

  await Promise.all(dataPromises);
  clearInterval(chatInterval);
  const dataTime = performance.now() - dataStart;
  const dataThroughput = (100 * 100) / (dataTime / 1000); // 100KB * 100 messages

  console.log(`Chat messages: ${chatMessages}`);
  console.log(`Data chunks: ${dataChunks}`);
  console.log(`Data transfer: 10MB in ${dataTime.toFixed(1)}ms (${(dataThroughput / 1024).toFixed(1)} MB/s)`);
  console.log('Mixed workload completed\n');

  await chatChannel.close();
  await dataChannel.close();
  await client.disconnect();
}

async function main() {
  const totalStart = performance.now();

  try {
    await testIsolatedChannels();
    await testIsolatedProxies();
    await testMixedWorkload();

    const totalElapsed = performance.now() - totalStart;
    console.log(`All isolated connection tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
  } catch (error) {
    console.error('Test failed:', error);
    process.exit(1);
  }
}

main().catch(console.error);
