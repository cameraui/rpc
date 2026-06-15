import { createRPCClient, rpcCallbacks } from '../src/index.js';

interface MechService {
  pullSteady(count: number, callbacks: { onItem: (idx: number) => void | Promise<void> }): AsyncGenerator<void>;
}

class MechServiceImpl implements MechService {
  async *pullSteady(count: number, callbacks: { onItem: (idx: number) => void | Promise<void> }) {
    for (let i = 0; i < count; i++) {
      callbacks.onItem(i);
      yield;
    }
  }
}

async function main() {
  console.log('Pull-Callback Mechanism Test\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-mech-server',
  });
  await server.connect();
  const unsubHandler = await server.registerHandler('mech', new MechServiceImpl(), { withoutDecorators: true });

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'pull-callback-mech-client',
  });
  await client.connect();

  const service = client.createProxy<MechService>('mech');

  const COUNT = 10;
  const HANDLER_DELAY_MS = 200;
  const handlerStarts: number[] = [];
  const handlerEnds: number[] = [];

  const cbs = rpcCallbacks(
    {
      onItem: async (idx: number) => {
        handlerStarts.push(performance.now());
        await new Promise((r) => setTimeout(r, HANDLER_DELAY_MS));
        handlerEnds.push(performance.now());
        console.log(`  onItem(${idx}) finished after ${HANDLER_DELAY_MS}ms`);
      },
    },
    { oneway: ['onItem'] },
  );

  console.log(
    `Consuming ${COUNT} items, each handler awaits ${HANDLER_DELAY_MS}ms.\n` +
      // eslint-disable-next-line @stylistic/indent-binary-ops
      'Consumer loop has no delay — only the handler blocks.\n',
  );

  const start = performance.now();

  for await (const _ of service.pullSteady(COUNT, cbs)) {
    // No delay in consumer — tight loop.
  }

  const elapsed = performance.now() - start;
  await new Promise((r) => setTimeout(r, 50));

  console.log('\nTiming');
  console.log(`  Handler invocations:  ${handlerStarts.length}`);
  console.log(`  Total elapsed:        ${elapsed.toFixed(1)}ms`);
  console.log(`  Expected (serialized): ${COUNT * HANDLER_DELAY_MS}ms`);
  console.log(`  Expected (no BP):      ~${COUNT * 2}ms (network only)`);

  // Check: no two handlers were running at the same time.
  let overlaps = 0;
  for (let i = 1; i < handlerStarts.length; i++) {
    if (handlerStarts[i] < handlerEnds[i - 1] - 5) overlaps++; // 5ms slack
  }
  console.log(`  Overlapping handlers:  ${overlaps} (expected 0)`);

  const expected = COUNT * HANDLER_DELAY_MS;
  const withinRange = elapsed >= expected * 0.9 && elapsed <= expected * 1.3;
  const ok = handlerStarts.length === COUNT && withinRange && overlaps === 0;

  console.log();
  if (ok) {
    console.log('PASS — callback handlers are serialized, backpressure propagates');
  } else {
    console.log('FAIL — handlers did not serialize, or timing does not match');
  }

  await unsubHandler();
  await client.disconnect();
  await server.disconnect();
}

main().catch(console.error);
