import { RPCClass } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

import type { RPCClient } from '../src/index.js';

@RPCClass
class ComputeService {
  async fibonacci(n: number): Promise<number> {
    console.log(`Computing fibonacci(${n})`);
    if (n <= 1) return n;

    let a = 0,
      b = 1;
    for (let i = 2; i <= n; i++) {
      const temp = a + b;
      a = b;
      b = temp;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
    return b;
  }

  async *generatePrimes(limit: number) {
    console.log(`Generating primes up to ${limit}`);

    for (let num = 2; num <= limit; num++) {
      let isPrime = true;

      for (let i = 2; i * i <= num; i++) {
        if (num % i === 0) {
          isPrime = false;
          break;
        }
      }

      if (isPrime) {
        yield num;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
  }
}

@RPCClass
class EchoService {
  async echo(message: string): Promise<string> {
    console.log(`Echo: ${message}`);
    return `Echo: ${message}`;
  }

  async ping(): Promise<string> {
    return 'pong';
  }
}

async function main() {
  const totalStart = performance.now();
  console.log('Starting server-side isolated service test...\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'service-server',
  });

  await server.connect();
  console.log('Server connected');

  console.log('Registering compute service with isolated connection...');
  const service1 = await server.service.registerHandler(
    {
      name: 'compute',
      version: '1.0.0',
      description: 'Heavy computation service (isolated)',
    },
    new ComputeService(),
    {
      isolatedConnection: true,
    },
  );

  console.log('Registering echo service on main connection...');
  const service2 = await server.service.registerHandler(
    {
      name: 'echo',
      version: '1.0.0',
      description: 'Lightweight echo service',
    },
    new EchoService(),
  );

  console.log('Services registered\n');

  const clients: RPCClient[] = [];
  const NUM_CLIENTS = 5;

  for (let i = 0; i < NUM_CLIENTS; i++) {
    const client = createRPCClient({
      servers: ['nats://localhost:4222'],
      auth: { user: 'server', password: 'server_password' },
      name: `test-client-${i}`,
    });
    await client.connect();
    clients.push(client);
  }
  console.log(`${NUM_CLIENTS} clients connected\n`);

  console.log('Testing Concurrent Heavy Computations');
  console.log('All clients will request fibonacci calculations simultaneously...\n');

  const computeProxies = await Promise.all(
    clients.map((client) =>
      client.createServiceProxy<{
        fibonacci(n: number): Promise<number>;
        generatePrimes(limit: number): AsyncGenerator<number>;
      }>('compute', { isolatedConnection: true }),
    ),
  );

  const echoProxies = await Promise.all(
    clients.map((client) =>
      client.createServiceProxy<{
        echo(message: string): Promise<string>;
        ping(): Promise<string>;
      }>('echo', { isolatedConnection: false }),
    ),
  );

  const computeStart = performance.now();
  const computePromises = computeProxies.map(({ proxy }, i) => {
    const n = 35 + i;
    console.log(`Client ${i}: Starting fibonacci(${n})`);
    const clientStart = performance.now();
    return proxy.fibonacci(n).then((result) => {
      const elapsed = performance.now() - clientStart;
      console.log(`Client ${i}: fibonacci(${n}) = ${result} (${elapsed.toFixed(1)}ms)`);
      return result;
    });
  });

  console.log('\nTesting echo service responsiveness during heavy load...');
  const echoTimes: number[][] = [];
  for (let round = 0; round < 3; round++) {
    await new Promise((resolve) => setTimeout(resolve, 100));

    const echoPromises = echoProxies.map(async (proxy, i) => {
      const start = performance.now();
      await proxy.echo(`Round ${round + 1} from client ${i}`);
      const elapsed = performance.now() - start;
      return { client: i, elapsed };
    });

    const results = await Promise.all(echoPromises);
    const roundTimes = results.map((r) => r.elapsed);
    echoTimes.push(roundTimes);
    console.log(`Round ${round + 1} echo times:`, results.map((r) => `${r.elapsed.toFixed(1)}ms`).join(', '));
  }

  const avgEchoTime = echoTimes.flat().reduce((a, b) => a + b, 0) / (echoTimes.length * NUM_CLIENTS);
  console.log(`Average echo time during heavy load: ${avgEchoTime.toFixed(1)}ms`);

  console.log('\nWaiting for all computations to complete...');
  await Promise.all(computePromises);
  const computeTime = performance.now() - computeStart;
  console.log(`All computations completed in ${computeTime.toFixed(1)}ms`);

  console.log('\nTesting Concurrent Streaming');
  const streamStart = performance.now();
  const streamPromises = computeProxies.slice(0, 3).map(async ({ proxy }, i) => {
    console.log(`Client ${i}: Starting prime generation`);
    const clientStreamStart = performance.now();
    const primes = proxy.generatePrimes(20);
    const collected: number[] = [];

    for await (const prime of primes) {
      collected.push(prime);
    }

    const elapsed = performance.now() - clientStreamStart;
    console.log(`Client ${i}: Collected ${collected.length} primes in ${elapsed.toFixed(1)}ms`);
    return collected;
  });

  await Promise.all(streamPromises);
  const streamTime = performance.now() - streamStart;
  console.log(`All streams completed in ${streamTime.toFixed(1)}ms`);

  console.log('\nConnection Status');
  console.log('Main server connection active:', server.isConnected);
  console.log('Isolated service connections managed by services');

  console.log('\nCleaning up...');
  await service1.stop();
  await service2.stop();
  await Promise.all(computeProxies.map((p) => p.close()));
  await Promise.all(clients.map((c) => c.disconnect()));
  await server.service.stopAll();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
