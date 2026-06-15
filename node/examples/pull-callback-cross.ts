import { createRPCClient, rpcCallbacks } from '../src/index.js';

interface DataService {
  pullBatches(batchCount: number, chunksPerBatch: number, callbacks: { onChunk: (data: { batch: number; index: number } | null) => void }): AsyncGenerator<void>;
}

class DataServiceImpl implements DataService {
  async *pullBatches(batchCount: number, chunksPerBatch: number, callbacks: { onChunk: (data: { batch: number; index: number } | null) => void }) {
    for (let b = 0; b < batchCount; b++) {
      for (let i = 0; i < chunksPerBatch; i++) {
        callbacks.onChunk({ batch: b, index: i });
      }
      callbacks.onChunk(null);
      yield;
    }
  }
}

function parseFlag(argv: string[], name: string): string | undefined {
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === name && i + 1 < argv.length) return argv[i + 1];
    if (argv[i].startsWith(`${name}=`)) return argv[i].slice(name.length + 1);
  }
  return undefined;
}

async function runServer(name: string) {
  console.log(`server ${name} starting...`);
  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: `pullcb-cross-node-server-${name}`,
  });
  await client.connect();
  const unsub = await client.registerHandler(`pullcb-${name}`, new DataServiceImpl(), { withoutDecorators: true });
  console.log(`server ${name} registered under namespace pullcb-${name}, ready.`);

  await new Promise<void>((resolve) => {
    const stop = async () => {
      console.log(`server ${name} shutting down...`);
      await unsub();
      await client.disconnect();
      resolve();
    };
    process.once('SIGTERM', stop);
    process.once('SIGINT', stop);
  });
}

async function testTarget(clientName: string, target: string): Promise<boolean> {
  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: `pullcb-cross-node-client-${clientName}-to-${target}`,
  });
  await client.connect();

  try {
    const service = client.createProxy<DataService>(`pullcb-${target}`);

    const received: { batch: number; index: number }[] = [];
    let batchEnds = 0;

    const cbs = rpcCallbacks(
      {
        onChunk: (data: { batch: number; index: number } | null) => {
          if (data === null) {
            batchEnds++;
            return;
          }
          received.push(data);
        },
      },
      { oneway: ['onChunk'] },
    );

    const BATCHES = 3;
    const CHUNKS = 5;

    let batchesConsumed = 0;
    for await (const _ of service.pullBatches(BATCHES, CHUNKS, cbs)) {
      batchesConsumed++;
    }

    // Allow trailing callback messages to land.
    await new Promise((r) => setTimeout(r, 100));

    const orderOk = received.every((r, i) => {
      const expectedBatch = Math.floor(i / CHUNKS);
      const expectedIdx = i % CHUNKS;
      return r.batch === expectedBatch && r.index === expectedIdx;
    });

    const ok = batchesConsumed === BATCHES && received.length === BATCHES * CHUNKS && batchEnds === BATCHES && orderOk;

    if (ok) {
      console.log(`  node -> ${target}: PASS (${received.length} chunks, ${batchEnds} EOB)`);
    } else {
      console.log(
        // eslint-disable-next-line @stylistic/max-len
        `  node -> ${target}: FAIL: consumed=${batchesConsumed}/${BATCHES}, chunks=${received.length}/${BATCHES * CHUNKS}, ends=${batchEnds}/${BATCHES}, orderOk=${orderOk}`,
      );
    }
    return ok;
  } finally {
    await client.disconnect();
  }
}

async function runClient(targets: string[]) {
  console.log(`testing targets: ${targets.join(', ')}`);
  const results: { target: string; ok: boolean }[] = [];
  for (const t of targets) {
    try {
      const ok = await testTarget('node', t);
      results.push({ target: t, ok });
    } catch (err) {
      console.log(`  node -> ${t}: ERROR: ${(err as Error).message}`);
      results.push({ target: t, ok: false });
    }
  }

  const failed = results.filter((r) => !r.ok);
  console.log('');
  if (failed.length === 0) {
    console.log(`all ${results.length} targets passed`);
    process.exit(0);
  } else {
    console.log(`${failed.length}/${results.length} targets failed`);
    process.exit(1);
  }
}

async function main() {
  const role = parseFlag(process.argv.slice(2), '--role') ?? 'client';
  if (role === 'server') {
    const name = parseFlag(process.argv.slice(2), '--name') ?? 'node';
    await runServer(name);
  } else {
    const targetsArg = parseFlag(process.argv.slice(2), '--targets') ?? 'node,go,python';
    const targets = targetsArg
      .split(',')
      .map((t) => t.trim())
      .filter(Boolean);
    await runClient(targets);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
