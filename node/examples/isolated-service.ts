import { RPCClass } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

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
  console.log('Starting isolated service connection test...\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'service-server',
  });

  await server.connect();
  console.log('Server connected');

  const service1 = await server.service.registerHandler(
    {
      name: 'compute',
      version: '1.0.0',
      description: 'Heavy computation service',
    },
    new ComputeService(),
  );

  const service2 = await server.service.registerHandler(
    {
      name: 'echo',
      version: '1.0.0',
      description: 'Lightweight echo service',
    },
    new EchoService(),
  );

  console.log('Services registered\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'test-client',
  });

  await client.connect();
  console.log('Client connected\n');

  console.log('Creating Service Proxies');

  // prettier-ignore
  const compute = await client.createServiceProxy<{
    fibonacci(n: number): Promise<number>;
    generatePrimes(limit: number): AsyncGenerator<number>;
  }>('compute', {
    isolatedConnection: true,
    timeout: 60000, // 60 seconds for heavy computations
  });
  console.log('Compute service proxy created (isolated connection)');

  const echo = await client.createServiceProxy<{
    echo(message: string): Promise<string>;
    ping(): Promise<string>;
  }>('echo');
  console.log('Echo service proxy created (shared connection)\n');

  console.log('Testing Concurrent Operations');

  console.log('Starting heavy computation (fibonacci)...');
  const fibStart = performance.now();
  const fibPromise = compute.proxy.fibonacci(40).then((result) => {
    const elapsed = performance.now() - fibStart;
    console.log(`Fibonacci(40) = ${result} (${elapsed.toFixed(1)}ms)`);
    return result;
  });

  console.log('\nTesting echo service while computing...');
  const echoTimes: number[] = [];
  for (let i = 1; i <= 5; i++) {
    const start = performance.now();
    const result = await echo.echo(`Message ${i}`);
    const elapsed = performance.now() - start;
    echoTimes.push(elapsed);
    console.log(`${result} (${elapsed.toFixed(1)}ms)`);
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  const avgEchoTime = echoTimes.reduce((a, b) => a + b, 0) / echoTimes.length;
  console.log(`Average echo time: ${avgEchoTime.toFixed(1)}ms`);

  console.log('\nWaiting for fibonacci computation...');
  await fibPromise;

  console.log('\nTesting Streaming with Isolation');
  const streamStart = performance.now();
  const primes = compute.proxy.generatePrimes(30);
  const collectedPrimes: number[] = [];

  console.log('Collecting primes while doing other work...');
  const primePromise = (async () => {
    for await (const prime of primes) {
      collectedPrimes.push(prime);
      console.log(`Received prime: ${prime}`);
    }
  })();

  for (let i = 1; i <= 3; i++) {
    await new Promise((resolve) => setTimeout(resolve, 100));
    const pong = await echo.ping();
    console.log(`Ping ${i}: ${pong}`);
  }

  await primePromise;
  const streamTime = performance.now() - streamStart;
  console.log(`\nCollected ${collectedPrimes.length} primes in ${streamTime.toFixed(1)}ms: ${collectedPrimes.join(', ')}`);

  console.log('\nTesting Connection Isolation');
  console.log('Disconnecting compute service (isolated)...');
  await compute.close();
  console.log('Isolated connection closed\n');

  console.log('Testing echo service after compute disconnect...');
  try {
    const start = performance.now();
    const result = await echo.echo('Still working?');
    const elapsed = performance.now() - start;
    console.log(`Echo service: ${result} (${elapsed.toFixed(1)}ms)`);
  } catch (error: any) {
    console.error('Echo failed:', error.message);
  }

  console.log('\nTesting compute service after disconnect...');
  try {
    await compute.proxy.fibonacci(10);
  } catch (error: any) {
    console.log('Expected error:', error.message);
  }

  console.log('\nCleaning up...');
  await service1.stop();
  await service2.stop();
  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
