import { RPCClass } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

const smallBuffer = Buffer.from('This is a small response that fits in a single message');
const mediumBuffer2MB = Buffer.alloc(2 * 1024 * 1024);
const mediumBuffer5MB = Buffer.alloc(5 * 1024 * 1024);
const largeBuffer10MB = Buffer.alloc(10 * 1024 * 1024, 'x');
const chunkBuffer1MB = Buffer.alloc(1 * 1024 * 1024);

for (let i = 0; i < mediumBuffer2MB.length; i++) {
  mediumBuffer2MB[i] = i % 256;
}
for (let i = 0; i < mediumBuffer5MB.length; i++) {
  mediumBuffer5MB[i] = i % 256;
}
for (let i = 0; i < chunkBuffer1MB.length; i++) {
  chunkBuffer1MB[i] = i % 256;
}

interface TestService {
  getSmallData(): Promise<string>;
  getMediumData(sizeMB: number): Promise<Buffer>;
  getLargeData(sizeMB: number): Promise<Buffer>;

  echo(data: Buffer): Promise<Buffer>;

  generateLargeDataStream(chunkSizeMB: number, chunks: number): AsyncGenerator<Buffer>;

  ping(): Promise<string>;
}

@RPCClass
class TestServiceImpl implements TestService {
  async getSmallData() {
    return smallBuffer.toString();
  }

  async getMediumData(sizeMB: number) {
    console.log(`[Service] Returning ${sizeMB}MB buffer`);
    if (sizeMB === 2) return mediumBuffer2MB;
    if (sizeMB === 5) return mediumBuffer5MB;
    const buffer = Buffer.alloc(sizeMB * 1024 * 1024);
    for (let i = 0; i < buffer.length; i++) {
      buffer[i] = i % 256;
    }
    return buffer;
  }

  async getLargeData(sizeMB: number) {
    return this.getMediumData(sizeMB);
  }

  async echo(data: Buffer) {
    console.log(`[Service] Echoing ${(data.length / 1024 / 1024).toFixed(2)}MB`);
    return data;
  }

  async *generateLargeDataStream(chunkSizeMB: number, chunks: number) {
    console.log(`[Service] Streaming ${chunks} chunks of ${chunkSizeMB}MB each`);

    for (let i = 0; i < chunks; i++) {
      if (chunkSizeMB === 1) {
        console.log(`[Service] Yielding chunk ${i + 1}/${chunks} - size: ${chunkBuffer1MB.length} bytes`);
        yield chunkBuffer1MB;
      } else {
        const buffer = Buffer.alloc(chunkSizeMB * 1024 * 1024);
        for (let j = 0; j < buffer.length; j++) {
          buffer[j] = (i + j) % 256;
        }
        console.log(`[Service] Yielding chunk ${i + 1}/${chunks} - size: ${buffer.length} bytes`);
        yield buffer;
      }

      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async ping() {
    return `pong at ${new Date().toISOString()}`;
  }
}

async function main() {
  const totalStart = performance.now();
  let isConnected = false;

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'server',
  });

  await server.connect();
  const unsub = await server.registerHandler('test', new TestServiceImpl());

  const clientA = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'client-a',
  });

  const clientB = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'client-b',
  });

  await clientA.connect();
  await clientB.connect();

  console.log('Clients connected');

  isConnected = true;

  const clientAChannel = await clientA.privateChannel('secret-chat', 'client-b');
  const clientBChannel = await clientB.privateChannel('secret-chat', 'client-a');

  console.log('Private channels created');

  clientAChannel.on('message', (data) => {
    if (data.file) {
      data.file = `<${data.file.length} bytes>`;
    }

    console.log('[Client A received]:', data);
  });

  clientBChannel.on('message', (data) => {
    if (data.file) {
      data.file = `<${data.file.length} bytes>`;
    }

    console.log('[Client B received]:', data);
  });

  const testInfiniteMessage = async () => {
    let count = 0;
    let currentClient = clientAChannel;
    const file = largeBuffer10MB;
    while (isConnected) {
      try {
        const from = currentClient === clientAChannel ? 'Client A' : 'Client B';
        await currentClient.send({ from, text: `Message ${count++}`, date: new Date().toISOString(), file });
      } catch (err) {
        if (!isConnected) {
          break;
        }

        console.error('Error sending message:', err);
      } finally {
        await new Promise((resolve) => setTimeout(resolve, 100));
        currentClient = currentClient === clientAChannel ? clientBChannel : clientAChannel;
      }
    }
  };

  testInfiniteMessage().catch((err) => {
    console.error('Error in infinite message test:', err);
  });

  const serviceA = clientA.createProxy<TestService>('test');

  console.log('\n=== Testing service methods ===');

  let start = performance.now();
  const smallData = await serviceA.getSmallData();
  let elapsed = performance.now() - start;
  console.log(`Small Data: ${smallData.length} bytes (${elapsed.toFixed(1)}ms)`);

  start = performance.now();
  const mediumData = await serviceA.getMediumData(2);
  elapsed = performance.now() - start;
  console.log(`Medium Data (2MB): ${mediumData.length} bytes (${elapsed.toFixed(1)}ms, ${(2000 / elapsed).toFixed(1)} MB/s)`);

  start = performance.now();
  const largeData = await serviceA.getLargeData(5);
  elapsed = performance.now() - start;
  console.log(`Large Data (5MB): ${largeData.length} bytes (${elapsed.toFixed(1)}ms, ${(5000 / elapsed).toFixed(1)} MB/s)`);

  start = performance.now();
  const echoData = await serviceA.echo(Buffer.from('Hello, World!'));
  elapsed = performance.now() - start;
  console.log(`Echo Data: ${echoData.length} bytes (${elapsed.toFixed(1)}ms)`);

  console.log('\n=== Testing streaming ===');
  start = performance.now();
  const generateLargeDataStream = serviceA.generateLargeDataStream(1, 100);
  let chunkCount = 0;
  for await (const chunk of generateLargeDataStream) {
    chunkCount++;
    if (chunkCount <= 3 || chunkCount > 97) {
      console.log(`Received Chunk ${chunkCount}: ${chunk.length} bytes`);
    } else if (chunkCount === 4) {
      console.log('... (skipping intermediate chunks) ...');
    }
  }
  elapsed = performance.now() - start;
  console.log(`Stream completed: ${chunkCount} chunks, total ${chunkCount}MB in ${elapsed.toFixed(1)}ms (${((chunkCount * 1000) / elapsed).toFixed(1)} MB/s)`);

  start = performance.now();
  const pingResponse = await serviceA.ping();
  elapsed = performance.now() - start;
  console.log(`\nPing Response: ${pingResponse} (${elapsed.toFixed(1)}ms)`);

  console.log('\n=== Running concurrent message test for 10 seconds ===');
  await new Promise((resolve) => setTimeout(resolve, 10000));
  console.log('\n=== Disconnecting ===');
  isConnected = false;
  await unsub();
  await clientAChannel.close();
  await clientBChannel.close();
  await clientA.disconnect();
  await clientB.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`Test completed. Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
