import { RPCClass, RPCNested } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

const buffer2MB = Buffer.alloc(2 * 1024 * 1024);
for (let i = 0; i < buffer2MB.length; i++) {
  buffer2MB[i] = i % 256;
}

@RPCClass
class MathService {
  async add(a: number, b: number) {
    return a + b;
  }

  async multiply(a: number, b: number) {
    return a * b;
  }

  async divide(a: number, b: number) {
    if (b === 0) throw new Error('Division by zero');
    return a / b;
  }
}

@RPCClass
class StringService {
  async concat(a: string, b: string) {
    return a + b;
  }

  async reverse(str: string) {
    return str.split('').reverse().join('');
  }

  async *generateWords(count: number) {
    const words = ['hello', 'world', 'nats', 'rpc', 'service'];
    for (let i = 0; i < count; i++) {
      yield words[i % words.length];
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
}

@RPCClass
class DataService {
  async createBuffer(sizeMB: number) {
    if (sizeMB === 2) {
      return buffer2MB;
    }
    const buffer = Buffer.alloc(sizeMB * 1024 * 1024);
    for (let i = 0; i < buffer.length; i++) {
      buffer[i] = i % 256;
    }
    return buffer;
  }

  @RPCNested
  info = {
    async version() {
      return '1.0.0';
    },

    async status() {
      return {
        healthy: true,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
      };
    },
  };
}

async function main() {
  const totalStart = performance.now();
  console.log('Starting multi-service test...\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'multi-service-server',
  });

  await server.connect();
  console.log('Server connected');

  const service1 = await server.service.registerHandler(
    {
      name: 'math',
      version: '1.0.0',
      description: 'Mathematical operations service',
      queue: 'math-workers',
    },
    new MathService(),
  );

  const service2 = await server.service.registerHandler(
    {
      name: 'string',
      version: '1.0.0',
      description: 'String manipulation service',
      queue: 'string-workers',
    },
    new StringService(),
  );

  const service3 = await server.service.registerHandler(
    {
      name: 'data',
      version: '2.0.0',
      description: 'Data generation and info service',
      metadata: {
        author: 'test',
        capabilities: 'buffer-generation,status-info',
      },
    },
    new DataService(),
  );

  console.log('\nRegistered services:');
  server.service.getAllInfo().forEach((info) => {
    console.log(`- ${info.name} v${info.version}: ${info.description}`);
  });

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'test-client',
  });

  await client.connect();
  console.log('\nClient connected');

  console.log('\nService Discovery');
  const monitor = server.service.monitor();

  for await (const info of await monitor.info()) {
    console.log(`Found: ${info.name} v${info.version} (${info.id})`);
  }

  console.log('\nMath Service');
  const mathStart = performance.now();
  const math = await client.createServiceProxy<{
    add(a: number, b: number): Promise<number>;
    multiply(a: number, b: number): Promise<number>;
    divide(a: number, b: number): Promise<number>;
  }>('math');
  const mathProxyTime = performance.now() - mathStart;
  console.log(`Math proxy created in ${mathProxyTime.toFixed(1)}ms`);

  let start = performance.now();
  console.log('5 + 3 =', await math.add(5, 3), `(${(performance.now() - start).toFixed(1)}ms)`);
  start = performance.now();
  console.log('4 * 7 =', await math.multiply(4, 7), `(${(performance.now() - start).toFixed(1)}ms)`);
  start = performance.now();
  console.log('10 / 2 =', await math.divide(10, 2), `(${(performance.now() - start).toFixed(1)}ms)`);

  console.log('\nString Service');
  const stringStart = performance.now();
  const string = await client.createServiceProxy<{
    concat(a: string, b: string): Promise<string>;
    reverse(str: string): Promise<string>;
    generateWords(count: number): AsyncGenerator<string>;
  }>('string');
  const stringProxyTime = performance.now() - stringStart;
  console.log(`String proxy created in ${stringProxyTime.toFixed(1)}ms`);

  start = performance.now();
  console.log('concat("Hello", "World") =', await string.concat('Hello', 'World'), `(${(performance.now() - start).toFixed(1)}ms)`);
  start = performance.now();
  console.log('reverse("NATS") =', await string.reverse('NATS'), `(${(performance.now() - start).toFixed(1)}ms)`);

  console.log('\nStreaming words:');
  const streamStart = performance.now();
  const words = string.generateWords(5);
  let wordCount = 0;
  for await (const word of words) {
    console.log(' -', word);
    wordCount++;
  }
  const streamTime = performance.now() - streamStart;
  console.log(`Streamed ${wordCount} words in ${streamTime.toFixed(1)}ms`);

  console.log('\nData Service');
  const dataStart = performance.now();
  const data = await client.createServiceProxy<{
    createBuffer(sizeMB: number): Promise<Buffer>;
    info: {
      version(): Promise<string>;
      status(): Promise<any>;
    };
  }>('data');
  const dataProxyTime = performance.now() - dataStart;
  console.log(`Data proxy created in ${dataProxyTime.toFixed(1)}ms`);

  start = performance.now();
  const buffer = await data.createBuffer(2);
  const bufferTime = performance.now() - start;
  const bufferLength = buffer?.length || 0;
  console.log(
    // eslint-disable-next-line @stylistic/max-len
    `Generated buffer: ${(bufferLength / 1024 / 1024).toFixed(2)}MB in ${bufferTime.toFixed(1)}ms (${(bufferLength / 1024 / 1024 / (bufferTime / 1000)).toFixed(1)} MB/s)`,
  );

  start = performance.now();
  console.log('Service version:', await data.info.version(), `(${(performance.now() - start).toFixed(1)}ms)`);
  start = performance.now();
  const status = await data.info.status();
  console.log('Service status:', {
    healthy: status.healthy,
    uptime: `${status.uptime.toFixed(2)}s`,
    memory: `${(status.memory.heapUsed / 1024 / 1024).toFixed(2)}MB`,
  });

  console.log('\nAll Services Stats');
  const allStats = await server.service.getAllStats();
  allStats.forEach((stats) => {
    console.log(`\n${stats.name} (${stats.id}):`);
    console.log(`  Started: ${stats.started}`);
    console.log(`  Total endpoints: ${stats.endpoints?.length ?? 0}`);
    const totalRequests = stats.endpoints?.reduce((sum, e) => sum + e.num_requests, 0) ?? 0;
    console.log(`  Total requests: ${totalRequests}`);
  });

  console.log('\nStopping string service');
  await server.service.stop('string');

  console.log('Remaining services:');
  server.service.getAllInfo().forEach((info) => {
    console.log(`- ${info.name} v${info.version}`);
  });

  try {
    console.log('\nTrying to use stopped string service...');
    await string.reverse('test');
  } catch (error: any) {
    console.log('Expected error:', error.message);
  }

  console.log('\nCleaning up...');
  await service1.stop();
  await service2.stop();
  await service3.stop();
  await new Promise((resolve) => setTimeout(resolve, 1000));
  await server.service.stopAll();
  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
