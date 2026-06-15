import { RPCClass } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

const buffer10MB = Buffer.alloc(10 * 1024 * 1024, 'x');
const buffer20MB = Buffer.alloc(20 * 1024 * 1024, 'x');

@RPCClass
class DataService {
  async *generateLargeData(sizeMb: number): AsyncGenerator<Buffer> {
    const chunkSize = 1024 * 1024;
    const totalSize = sizeMb * 1024 * 1024;
    let sent = 0;

    console.log(`[Server] Starting to stream ${sizeMb}MB of data...`);

    while (sent < totalSize) {
      const remaining = totalSize - sent;
      const currentChunkSize = Math.min(chunkSize, remaining);
      const chunk = Buffer.alloc(currentChunkSize).fill('x');
      sent += currentChunkSize;

      yield chunk;

      const progress = Math.floor((sent / totalSize) * 100);
      if (progress % 10 === 0 && progress > 0 && progress !== 100) {
        console.log(`[Server] Streamed ${progress}%`);
      }
    }

    console.log('[Server] Streaming completed');
  }
}

async function main() {
  const totalStart = performance.now();
  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'server',
  });

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'client',
  });

  await server.connect();
  await client.connect();

  console.log('Connected to NATS\n');
  console.log(`Server max payload: ${(server.maxPayloadSize / 1024 / 1024).toFixed(1)}MB`);
  console.log(`Client max payload: ${(client.maxPayloadSize / 1024 / 1024).toFixed(1)}MB\n`);

  console.log('Method 1: Publish/Subscribe with Auto-Chunking\n');

  let receivedData: any = null;

  const unsub = await server.subscribe<{ payload: Buffer; checksum: number }>('large.data.transfer', (data) => {
    receivedData = data;
    console.log(`Received ${(data.payload.length / 1024 / 1024).toFixed(1)}MB via pub/sub`);
  });

  const largePayload = buffer10MB;
  console.log(`Sending ${(largePayload.length / 1024 / 1024).toFixed(1)}MB via publish...`);
  const start = performance.now();
  await client.publish('large.data.transfer', { payload: largePayload, checksum: largePayload.length });

  await new Promise((resolve) => setTimeout(resolve, 500));
  const elapsed = performance.now() - start;

  if (receivedData) {
    console.log(`Transfer completed in ${elapsed.toFixed(1)}ms`);
    console.log(`Checksum verified: ${receivedData.checksum === largePayload.length}\n`);
  } else {
    console.log('Data not received\n');
  }

  unsub();

  console.log('Method 2: RPC Streaming for Large Data\n');

  const unregister = await server.registerHandler('data', new DataService());

  const dataService = client.createProxy<DataService>('data');

  console.log('Requesting 10MB of streamed data...');
  const streamStart = performance.now();
  let totalReceived = 0;

  for await (const chunk of dataService.generateLargeData(10)) {
    totalReceived += chunk.length;
  }

  const streamElapsed = performance.now() - streamStart;
  console.log(`Received ${(totalReceived / 1024 / 1024).toFixed(1)}MB in ${streamElapsed.toFixed(1)}ms`);
  console.log(`Transfer rate: ${(totalReceived / 1024 / 1024 / (streamElapsed / 1000)).toFixed(1)}MB/s\n`);

  await unregister();

  console.log('Method 3: Channels for Bidirectional Transfer\n');

  const serverChannel = await server.channel('large-data-channel');
  const clientChannel = await client.channel('large-data-channel');

  let serverReceived = 0;

  serverChannel.on('message', async (data: any) => {
    if ('chunk' in data) {
      serverReceived += data.chunk.length;
      if (data.last) {
        console.log(`Received total: ${(serverReceived / 1024 / 1024).toFixed(1)}MB`);
        await serverChannel.send({ received: serverReceived, status: 'complete' });
      }
    }
  });

  console.log('Sending 5MB through channel in chunks...');
  const channelStart = performance.now();
  const chunkSize = 512 * 1024;
  const totalSize = 5 * 1024 * 1024;
  let sent = 0;

  while (sent < totalSize) {
    const remaining = totalSize - sent;
    const currentChunkSize = Math.min(chunkSize, remaining);
    const chunk = Buffer.alloc(currentChunkSize).fill('x');
    sent += currentChunkSize;

    await clientChannel.send({ chunk, last: sent >= totalSize });
  }

  let confirmed = false;

  clientChannel.on('message', (data: any) => {
    if (data.status === 'complete') {
      confirmed = true;
      const channelElapsed = performance.now() - channelStart;
      console.log(`Channel transfer completed in ${channelElapsed.toFixed(1)}ms`);
      console.log(`Server confirmed ${(data.received / 1024 / 1024).toFixed(1)}MB received\n`);
    }
  });

  for (let i = 0; i < 20; i++) {
    if (confirmed) break;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  console.log('Method 4: Request/Reply for Large Data\n');

  try {
    console.log('Sending 20MB via request/reply (should fail)...');
    const hugePayload = buffer20MB;
    const reqStart = performance.now();
    const response = await client.request('large.data.reply', {
      payload: hugePayload,
      checksum: hugePayload.length,
    });
    const reqElapsed = performance.now() - reqStart;
    console.log(`Received response in ${reqElapsed.toFixed(1)}ms`);
    console.log(`Checksum verified: ${response.checksum === hugePayload.length}\n`);
  } catch (error: any) {
    console.log(`Request/Reply failed: ${error.message}\n`);
    console.log('This method is not suitable for large data transfers due to NATS max_payload limits.\n');
  }

  await serverChannel.close();
  await clientChannel.close();

  console.log('\nSummary');
  console.log('1. Publish/Subscribe: Best for one-way large data broadcasts');
  console.log('2. RPC Streaming: Best for controlled data flow with backpressure');
  console.log('3. Channels: Best for bidirectional communication with large data');
  console.log('4. Request/Reply: Limited by NATS max_payload, no auto-chunking');

  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
