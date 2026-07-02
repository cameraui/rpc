import { createRPCClient, rpcCallbacks } from '../src/index.js';

// Hot-path benchmark: measures the production-critical RPC paths
// (NVR frame delivery, parallel/sequential calls, channel throughput).
// Setup happens outside the measurements; each section runs one
// unmeasured warmup round first.

const ONEWAY_BATCHES = 5;
const ONEWAY_FRAMES_PER_BATCH = 1000;
const PARALLEL_CALLS = 500;
const SEQUENTIAL_CALLS = 200;
const CHANNEL_MESSAGES = 2000;

interface FrameService {
  pullFrames(batches: number, framesPerBatch: number, frameSize: number, callbacks: { onFrame: (frame: Uint8Array) => void }): AsyncGenerator<void>;
}

class FrameServiceImpl {
  // Method name contains "pull" so it routes through pull-callback mode.
  async *pullFrames(batches: number, framesPerBatch: number, frameSize: number, callbacks: { onFrame: (frame: Uint8Array) => void }) {
    const frame = new Uint8Array(frameSize);
    for (let i = 0; i < frame.length; i++) {
      frame[i] = i % 256;
    }
    for (let b = 0; b < batches; b++) {
      for (let i = 0; i < framesPerBatch; i++) {
        callbacks.onFrame(frame);
      }
      yield;
    }
  }
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(condition: () => boolean, timeoutMs = 30000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error('Timeout waiting for condition');
    }
    await sleep(1);
  }
}

async function runOneway(proxy: FrameService, frameSize: number, batches: number, framesPerBatch: number): Promise<{ elapsedMs: number; bytes: number }> {
  const expected = batches * framesPerBatch;
  let frames = 0;
  let bytes = 0;

  const cbs = rpcCallbacks(
    {
      onFrame: (frame: Uint8Array) => {
        frames++;
        bytes += frame.length;
      },
    },
    { oneway: ['onFrame'] },
  );

  const start = performance.now();
  for await (const _ of proxy.pullFrames(batches, framesPerBatch, frameSize, cbs)) {
    // batch boundary
  }
  await waitFor(() => frames >= expected);
  const elapsedMs = performance.now() - start;

  if (frames !== expected) {
    throw new Error(`Oneway frame count mismatch: got ${frames}, expected ${expected}`);
  }
  if (bytes !== expected * frameSize) {
    throw new Error(`Oneway byte count mismatch: got ${bytes}, expected ${expected * frameSize}`);
  }

  return { elapsedMs, bytes };
}

async function main(): Promise<void> {
  console.log('Perf Hotpath Benchmark (Node.js)\n');

  // --- Setup (not measured) ---
  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'perf-hotpath-server',
    auth: { user: 'server', password: 'server_password' },
  });
  await server.connect();

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'perf-hotpath-client',
    auth: { user: 'server', password: 'server_password' },
  });
  await client.connect();

  const unsubFrames = await server.registerHandler('hotpath-frames', new FrameServiceImpl(), { withoutDecorators: true });

  const echoHandlers = {
    echo: async (obj: Record<string, unknown>) => obj,
  };
  const unsubEcho = await server.registerHandler('hotpath-rpc', echoHandlers);
  await sleep(50); // Let subscriptions settle

  const frameProxy = client.createProxy<FrameService>('hotpath-frames');
  const echoProxy = client.createProxy<typeof echoHandlers>('hotpath-rpc');

  // --- 1. Oneway callback throughput (NVR frame path) ---
  console.log('1. Oneway Callback Throughput (pull-callback iterator)');

  await runOneway(frameProxy, 1024, 1, 50); // warmup (not measured)
  const oneway1k = await runOneway(frameProxy, 1024, ONEWAY_BATCHES, ONEWAY_FRAMES_PER_BATCH);
  const msgsPerSec = Math.round((ONEWAY_BATCHES * ONEWAY_FRAMES_PER_BATCH) / (oneway1k.elapsedMs / 1000));
  console.log(`Oneway 1KB: ${oneway1k.elapsedMs.toFixed(2)}ms (${msgsPerSec} msg/s)`);

  await runOneway(frameProxy, 100 * 1024, 1, 50); // warmup (not measured)
  const oneway100k = await runOneway(frameProxy, 100 * 1024, ONEWAY_BATCHES, ONEWAY_FRAMES_PER_BATCH);
  const mbPerSec = oneway100k.bytes / (1024 * 1024) / (oneway100k.elapsedMs / 1000);
  console.log(`Oneway 100KB: ${oneway100k.elapsedMs.toFixed(2)}ms (${mbPerSec.toFixed(1)} MB/s)`);

  // --- 2. Parallel calls ---
  console.log('\n2. Parallel Calls');

  const echoCall = async (i: number) => {
    const result = await echoProxy.echo({ seq: i, camera: 'cam-01', kind: 'echo' });
    if ((result as any).seq !== i) {
      throw new Error(`Echo seq mismatch at ${i}`);
    }
  };

  await Promise.all(Array.from({ length: 20 }, (_, i) => echoCall(i))); // warmup (not measured)

  const parallelStart = performance.now();
  await Promise.all(Array.from({ length: PARALLEL_CALLS }, (_, i) => echoCall(i)));
  const parallelMs = performance.now() - parallelStart;
  console.log(`Parallel ${PARALLEL_CALLS} calls: ${parallelMs.toFixed(2)}ms`);

  // --- 3. Sequential calls ---
  console.log('\n3. Sequential Calls');

  for (let i = 0; i < 10; i++) {
    await echoCall(i); // warmup (not measured)
  }

  const seqStart = performance.now();
  for (let i = 0; i < SEQUENTIAL_CALLS; i++) {
    await echoCall(i);
  }
  const seqMs = performance.now() - seqStart;
  const usPerCall = (seqMs * 1000) / SEQUENTIAL_CALLS;
  console.log(`Sequential ${SEQUENTIAL_CALLS} calls: ${seqMs.toFixed(2)}ms (${usPerCall.toFixed(1)} µs/call)`);

  // --- 4. Channel throughput ---
  console.log('\n4. Channel Throughput');

  const serverChannel = await server.channel('hotpath-channel');
  const clientChannel = await client.channel('hotpath-channel');

  let channelReceived = 0;
  serverChannel.on('message', () => {
    channelReceived++;
  });
  await sleep(50); // Let subscription settle

  // Warmup (not measured)
  for (let i = 0; i < 10; i++) {
    await clientChannel.send({ index: i, warmup: true });
  }
  await waitFor(() => channelReceived >= 10);

  const channelTarget = channelReceived + CHANNEL_MESSAGES;
  const channelStart = performance.now();
  for (let i = 0; i < CHANNEL_MESSAGES; i++) {
    await clientChannel.send({ index: i });
  }
  await waitFor(() => channelReceived >= channelTarget);
  const channelMs = performance.now() - channelStart;
  const usPerMsg = (channelMs * 1000) / CHANNEL_MESSAGES;
  console.log(`Channel ${CHANNEL_MESSAGES} msgs: ${channelMs.toFixed(2)}ms (${usPerMsg.toFixed(1)} µs/msg)`);

  await serverChannel.close();
  await clientChannel.close();

  // --- Cleanup ---
  await unsubFrames();
  await unsubEcho();
  await client.disconnect();
  await server.disconnect();

  console.log('\nAll hotpath benchmarks completed successfully!');
}

main().catch((error) => {
  console.error('Benchmark failed:', error);
  process.exit(1);
});
