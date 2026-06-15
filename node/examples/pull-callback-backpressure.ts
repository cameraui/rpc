import { createRPCClient, rpcCallbacks } from '../src/index.js';

interface DataService {
  pullPacedBatches(
    batchCount: number,
    chunksPerBatch: number,
    callbacks: {
      onChunk: (data: { batch: number; index: number; ts: number } | null) => void;
    },
  ): AsyncGenerator<void>;
}

class DataServiceImpl {
  async *pullPacedBatches(
    batchCount: number,
    chunksPerBatch: number,
    callbacks: {
      onChunk: (data: { batch: number; index: number; ts: number } | null) => void;
    },
  ) {
    for (let b = 0; b < batchCount; b++) {
      const batchStart = performance.now();
      for (let i = 0; i < chunksPerBatch; i++) {
        callbacks.onChunk({ batch: b, index: i, ts: performance.now() });
      }
      callbacks.onChunk(null);
      const produceTime = performance.now() - batchStart;
      console.log(`[Server] Batch ${b} produced in ${produceTime.toFixed(1)}ms, suspending at yield...`);
      yield;
      console.log(`[Server] Batch ${b} resumed (client called next)`);
    }
  }
}

async function main() {
  console.log('Pull-Callback Backpressure Example\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-bp-server',
  });
  await server.connect();
  const unsubHandler = await server.registerHandler('data', new DataServiceImpl(), { withoutDecorators: true });

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-bp-client',
  });
  await client.connect();

  const service = client.createProxy<DataService>('data');

  const BATCHES = 4;
  const CHUNKS = 1000;
  const CLIENT_DELAY_MS = 500;

  // Track first/last chunk timestamp per batch (server-side performance.now())
  const batchStats: { firstTs: number; lastTs: number; count: number }[] = [];
  for (let i = 0; i < BATCHES; i++) batchStats.push({ firstTs: -1, lastTs: -1, count: 0 });

  const cbs = rpcCallbacks(
    {
      onChunk: (data: { batch: number; index: number; ts: number } | null) => {
        if (data === null) return;
        const s = batchStats[data.batch];
        if (s.firstTs === -1) s.firstTs = data.ts;
        s.lastTs = data.ts;
        s.count++;
      },
    },
    { oneway: ['onChunk'] },
  );

  console.log(`Consuming ${BATCHES} batches x ${CHUNKS} chunks, client delays ${CLIENT_DELAY_MS}ms between batches...\n`);
  const start = performance.now();

  let idx = 0;
  for await (const _ of service.pullPacedBatches(BATCHES, CHUNKS, cbs)) {
    console.log(`[Client] Received batch boundary ${idx}, sleeping ${CLIENT_DELAY_MS}ms...`);
    await new Promise((resolve) => setTimeout(resolve, CLIENT_DELAY_MS));
    idx++;
  }

  const elapsed = performance.now() - start;
  await new Promise((resolve) => setTimeout(resolve, 50));

  console.log('\nPer-Batch Stats (server-side timestamps)');
  for (let i = 0; i < BATCHES; i++) {
    const s = batchStats[i];
    console.log(`  Batch ${i}: ${s.count} chunks, produce window [${s.firstTs.toFixed(0)}..${s.lastTs.toFixed(0)}] (${(s.lastTs - s.firstTs).toFixed(1)}ms)`);
  }

  console.log('\nInter-Batch Gaps (backpressure evidence)');
  let bpOk = true;
  for (let i = 1; i < BATCHES; i++) {
    const gap = batchStats[i].firstTs - batchStats[i - 1].lastTs;
    const expected = CLIENT_DELAY_MS;
    const withinRange = gap >= expected * 0.8; // allow 20% slack
    if (!withinRange) bpOk = false;
    console.log(`  Gap between batch ${i - 1} and ${i}: ${gap.toFixed(1)}ms (expected >=${expected * 0.8}ms) ${withinRange ? 'OK' : 'FAIL'}`);
  }

  console.log(`\nTotal elapsed: ${elapsed.toFixed(1)}ms (expected ~${BATCHES * CLIENT_DELAY_MS}ms)`);
  console.log(`\n${bpOk ? 'PASS' : 'FAIL'} - server paces at client's rhythm, not its own`);

  await unsubHandler();
  await client.disconnect();
  await server.disconnect();
}

main().catch(console.error);
