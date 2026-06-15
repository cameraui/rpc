import { createRPCClient, rpcCallbacks } from '../src/index.js';

interface DataService {
  // Method name must contain "pull" to route to pull-callback mode.
  pullBatches(batchCount: number, chunksPerBatch: number, callbacks: { onChunk: (data: { batch: number; index: number } | null) => void }): AsyncGenerator<void>;
}

class DataServiceImpl {
  async *pullBatches(batchCount: number, chunksPerBatch: number, callbacks: { onChunk: (data: { batch: number; index: number } | null) => void }) {
    console.log(`pullBatches(${batchCount}, ${chunksPerBatch}) started`);
    for (let b = 0; b < batchCount; b++) {
      for (let i = 0; i < chunksPerBatch; i++) {
        callbacks.onChunk({ batch: b, index: i });
      }
      callbacks.onChunk(null);
      console.log(`Batch ${b} produced, yielding...`);
      yield;
      console.log(`Resumed after batch ${b}`);
    }
    console.log('Generator complete');
  }
}

async function main() {
  const totalStart = performance.now();

  console.log('Pull-Callback Basic Example\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-basic-server',
  });
  await server.connect();

  const unsubHandler = await server.registerHandler('data', new DataServiceImpl(), { withoutDecorators: true });
  console.log('Server connected, handler registered\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-basic-client',
  });
  await client.connect();
  console.log('Client connected\n');

  const service = client.createProxy<DataService>('data');

  const received: { batch: number; index: number }[] = [];
  let batchEnds = 0;

  const cbs = rpcCallbacks(
    {
      onChunk: (data: { batch: number; index: number } | null) => {
        if (data === null) {
          batchEnds++;
          console.log(`Batch ${batchEnds - 1} end-of-batch sentinel received`);
          return;
        }
        received.push(data);
      },
    },
    { oneway: ['onChunk'] },
  );

  const BATCHES = 3;
  const CHUNKS = 5;

  console.log(`Starting pull iteration for ${BATCHES} batches x ${CHUNKS} chunks...\n`);
  const start = performance.now();

  let batchesConsumed = 0;
  for await (const _ of service.pullBatches(BATCHES, CHUNKS, cbs)) {
    batchesConsumed++;
    console.log(`Batch ${batchesConsumed - 1} boundary crossed (iteration ${batchesConsumed}/${BATCHES})`);
  }

  const elapsed = performance.now() - start;

  // Give any in-flight callback messages one more tick to land.
  await new Promise((resolve) => setTimeout(resolve, 50));

  console.log('\nResults');
  console.log(`  Batches consumed:  ${batchesConsumed} (expected ${BATCHES})`);
  console.log(`  Chunks received:   ${received.length} (expected ${BATCHES * CHUNKS})`);
  console.log(`  End-of-batch marks: ${batchEnds} (expected ${BATCHES})`);
  console.log(`  Total elapsed:     ${elapsed.toFixed(1)}ms`);

  const ok =
    batchesConsumed === BATCHES &&
    received.length === BATCHES * CHUNKS &&
    batchEnds === BATCHES &&
    received.every((r, i) => r.batch === Math.floor(i / CHUNKS) && r.index === i % CHUNKS);

  console.log(`\n${ok ? 'PASS' : 'FAIL'} — correctness check`);

  await unsubHandler();
  await client.disconnect();
  await server.disconnect();

  console.log(`\nTotal test time: ${(performance.now() - totalStart).toFixed(1)}ms`);
}

main().catch(console.error);
