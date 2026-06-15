import { createRPCClient } from '../src/index.js';

const VALID = ['nats://localhost:4222'];

function assert(cond: unknown, msg: string): void {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function main(): Promise<void> {
  const server = createRPCClient({
    servers: VALID,
    auth: { user: 'server', password: 'server_password' },
    name: 'reconfigure-test-server',
  });

  const client = createRPCClient({
    servers: VALID,
    auth: { user: 'server', password: 'server_password' },
    name: 'reconfigure-test-client',
  });

  await server.connect();
  await client.connect();
  console.log('Initial connect ok');

  let received = 0;
  await client.subscribe('reconfigure.test', (data: { n: number }) => {
    received += data.n;
  });

  await server.publish('reconfigure.test', { n: 1 });
  await new Promise((r) => setTimeout(r, 100));
  assert(received === 1, `pre-suspend publish (got ${received}, want 1)`);
  console.log('Pre-suspend publish delivered');

  let threw = false;
  try {
    client.reconfigure({ servers: VALID });
  } catch {
    threw = true;
  }
  assert(threw, 'reconfigure while connected must throw');
  console.log('reconfigure while connected throws');

  await client.suspend();
  client.reconfigure({ servers: VALID });
  await client.connect();

  received = 0;
  await server.publish('reconfigure.test', { n: 5 });
  await new Promise((r) => setTimeout(r, 100));
  assert(received === 5, `post-reconfigure publish (got ${received}, want 5)`);
  console.log('Subscriptions auto-restored after suspend, reconfigure, connect');

  await client.suspend();
  client.reconfigure({ auth: { user: 'server', password: 'server_password' } });
  await client.connect();
  received = 0;
  await server.publish('reconfigure.test', { n: 9 });
  await new Promise((r) => setTimeout(r, 100));
  assert(received === 9, `post-auth-replacement publish (got ${received}, want 9)`);
  console.log('reconfigure with auth replacement keeps subscriptions live');

  await client.suspend();
  client.reconfigure({ servers: ['nats://other:4222'] });

  const opts = (client as any).options as { servers: string[] };
  assert(opts.servers[0] === 'nats://other:4222', `servers mutated (got ${opts.servers[0]})`);
  console.log('options.servers reflects reconfigure');

  await client.disconnect();
  await server.disconnect();
  console.log('\nALL RECONFIGURE TESTS PASSED');
}

main().catch((err) => {
  console.error('TEST FAILED:', err);
  process.exit(1);
});
