import { createRPCClient, rpcCallbacks } from '../src/index.js';

interface DataService {
  pullForever(callbacks: { onChunk: (data: { batch: number; index: number }) => void }): AsyncGenerator<void>;
}

class DataServiceImpl {
  public teardownCount = 0;
  public producedBatches = 0;

  async *pullForever(callbacks: { onChunk: (data: { batch: number; index: number }) => void }) {
    console.log('pullForever started');
    try {
      for (let b = 0; ; b++) {
        for (let i = 0; i < 3; i++) {
          callbacks.onChunk({ batch: b, index: i });
        }
        this.producedBatches = b + 1;
        console.log(`Batch ${b} yielded`);
        yield;
      }
    } finally {
      this.teardownCount++;
      console.log(`Handler finally{} ran (teardownCount=${this.teardownCount})`);
    }
  }
}

async function main() {
  console.log('Pull-Callback Cancellation Example\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-cancel-server',
  });
  await server.connect();

  const impl = new DataServiceImpl();
  const unsubHandler = await server.registerHandler('data', impl, { withoutDecorators: true });

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-cancel-client',
  });
  await client.connect();

  const service = client.createProxy<DataService>('data');

  let chunksBeforeBreak = 0;
  let chunksAfterBreak = 0;
  let didBreak = false;

  const cbs = rpcCallbacks(
    {
      onChunk: (_data: { batch: number; index: number }) => {
        if (didBreak) chunksAfterBreak++;
        else chunksBeforeBreak++;
      },
    },
    { oneway: ['onChunk'] },
  );

  console.log('Starting infinite generator, breaking after 3 batches...\n');

  let batchesConsumed = 0;
  for await (const _ of service.pullForever(cbs)) {
    batchesConsumed++;
    console.log(`Batch ${batchesConsumed - 1} consumed`);
    if (batchesConsumed >= 3) {
      console.log('break!');
      didBreak = true;
      break;
    }
  }

  // Wait briefly for any straggler callback messages.
  await new Promise((resolve) => setTimeout(resolve, 300));

  console.log('\nResults');
  console.log(`  Batches consumed on client:   ${batchesConsumed} (expected 3)`);
  console.log(`  Batches produced on server:   ${impl.producedBatches} (expected 3 or 4, +1 tolerated for in-flight)`);
  console.log(`  Chunks received before break: ${chunksBeforeBreak}`);
  console.log(`  Chunks received after break:  ${chunksAfterBreak} (expected 0)`);
  console.log(`  Handler teardown count:       ${impl.teardownCount} (expected 1)`);

  const ok = batchesConsumed === 3 && impl.teardownCount === 1 && impl.producedBatches <= 4 && chunksAfterBreak === 0;

  console.log(`\n${ok ? 'PASS' : 'FAIL'} — cancellation cleanly stops server + callbacks`);

  await unsubHandler();
  await client.disconnect();
  await server.disconnect();
}

main().catch(console.error);
