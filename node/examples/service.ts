import { RPCClass, RPCNested } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

const buffer5MB = Buffer.alloc(5 * 1024 * 1024);
for (let i = 0; i < buffer5MB.length; i++) {
  buffer5MB[i] = i % 256;
}

@RPCClass
class ComputeService {
  private name = 'GPU Compute Node 1';

  async add(a: number, b: number) {
    return a + b;
  }

  async createData(sizeMB: number) {
    if (sizeMB === 5) {
      return buffer5MB;
    }
    const buffer = Buffer.alloc(sizeMB * 1024 * 1024);
    for (let i = 0; i < buffer.length; i++) {
      buffer[i] = i % 256;
    }
    return buffer;
  }

  async *generateNumbers(count: number) {
    for (let i = 0; i < count; i++) {
      const data = Buffer.alloc(1024 * 1024);
      data.fill(i % 256);

      yield {
        index: i,
        data,
        timestamp: performance.now(),
      };

      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  @RPCNested
  config = {
    get: async (): Promise<any> => {
      return {
        name: this.name,
        maxConcurrent: 10,
        gpu: true,
      };
    },

    set: async (config: any) => {
      if (config.name) this.name = config.name;
      return { success: true };
    },

    monitor: {
      async cpu() {
        return { usage: Math.random() * 100 };
      },

      async memory() {
        return {
          used: Math.random() * 16 * 1024,
          total: 16 * 1024,
        };
      },
    },
  };

  async failingMethod() {
    throw new Error('This method always fails');
  }
}

async function main() {
  const totalStart = performance.now();
  console.log('Starting service test...\n');

  const server1 = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'compute-server-1',
  });

  await server1.connect();
  console.log('Server 1 connected');

  const service1 = await server1.service.registerHandler(
    {
      name: 'compute',
      version: '1.0.0',
      description: 'High-performance compute service',
      queue: 'compute-workers', // Enable load balancing
      metadata: {
        host: 'server1',
        gpu: 'true',
        region: 'us-east',
      },
    },
    new ComputeService(),
  );

  console.log('Service 1 registered:', service1.info());

  const server2 = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'compute-server-2',
  });

  await server2.connect();

  const computeService2 = new ComputeService();
  await computeService2.config.set({ name: 'GPU Compute Node 2' });

  const service2 = await server2.service.registerHandler(
    {
      name: 'compute',
      version: '1.0.0',
      description: 'High-performance compute service',
      queue: 'compute-workers', // Same queue for load balancing
      metadata: {
        host: 'server2',
        gpu: 'false',
        region: 'eu-west',
      },
    },
    computeService2,
  );

  console.log('Service 2 registered:', service2.info());

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'test-client',
  });

  await client.connect();
  console.log('\nClient connected');

  console.log('\nService Discovery');
  const monitor = server1.service.monitor();

  for await (const info of await monitor.info('compute')) {
    console.log(`Found service: ${info.id}`);
    console.log(`  Version: ${info.version}`);
    console.log('  Metadata:', info.metadata);
    console.log(
      '  Endpoints:',
      info.endpoints.map((e: any) => e.subject),
    );
  }

  console.log('\nTesting Service Calls');
  const proxyStart = performance.now();
  const compute = await client.createServiceProxy<{
    add(a: number, b: number): Promise<number>;
    createData(sizeMB: number): Promise<Buffer>;
    generateNumbers(count: number): AsyncGenerator<any>;
    config: {
      get(): Promise<any>;
      set(config: any): Promise<any>;
      monitor: {
        cpu(): Promise<any>;
        memory(): Promise<any>;
      };
    };
    failingMethod(): Promise<void>;
  }>('compute');
  const proxyTime = performance.now() - proxyStart;
  console.log(`Service proxy created in ${proxyTime.toFixed(1)}ms`);

  let start = performance.now();
  const sum = await compute.add(5, 3);
  const addTime = performance.now() - start;
  console.log(`Add result: ${sum} (${addTime.toFixed(1)}ms)`);

  start = performance.now();
  const config = await compute.config.get();
  const configTime = performance.now() - start;
  console.log(`Config: ${JSON.stringify(config)} (${configTime.toFixed(1)}ms)`);

  start = performance.now();
  const cpu = await compute.config.monitor.cpu();
  const cpuTime = performance.now() - start;
  console.log(`CPU usage: ${JSON.stringify(cpu)} (${cpuTime.toFixed(1)}ms)`);

  console.log('\nTesting Large Data Transfer');
  start = performance.now();
  const largeData = await compute.createData(5);
  const dataTime = performance.now() - start;
  const throughput = (5 * 1024 * 1024) / ((dataTime / 1000) * 1024 * 1024);
  console.log(`Received ${(largeData?.length || 0) / 1024 / 1024}MB of data in ${dataTime.toFixed(1)}ms (${throughput.toFixed(1)} MB/s)`);

  console.log('\nTesting Streaming');
  const streamStart = performance.now();
  const stream = compute.generateNumbers(5);
  let streamCount = 0;
  for await (const item of stream) {
    console.log(`Stream item ${item.index}: ${(item.data.length / 1024 / 1024).toFixed(2)}MB`);
    streamCount++;
  }
  const streamTime = performance.now() - streamStart;
  console.log(`Received ${streamCount} stream items in ${streamTime.toFixed(1)}ms`);

  console.log('\nTesting Error Handling');
  start = performance.now();
  try {
    await compute.failingMethod();
  } catch (error: any) {
    const errorTime = performance.now() - start;
    console.log(`Caught expected error: ${error} (${errorTime.toFixed(1)}ms)`);
  }

  console.log('\nTesting Load Balancing');
  console.log('Making 10 requests to see distribution...');
  const lbStart = performance.now();
  for (let i = 0; i < 10; i++) {
    const reqStart = performance.now();
    const config = await compute.config.get();
    const reqTime = performance.now() - reqStart;
    console.log(`Request ${i + 1}: Handled by ${config.name} (${reqTime.toFixed(1)}ms)`);
  }
  const lbTime = performance.now() - lbStart;
  console.log(`Total load balancing test time: ${lbTime.toFixed(1)}ms`);

  console.log('\nService Stats');
  for await (const stats of await monitor.stats('compute')) {
    console.log(`Stats for ${stats.id}:`);
    console.log(`  Started: ${stats.started}`);
    console.log(
      '  Endpoints:',
      stats.endpoints?.map((e) => ({
        name: e.name,
        requests: e.num_requests,
        errors: e.num_errors,
        avgTime: `${((e as any).average_processing_time / 1000000).toFixed(2)}ms`,
      })),
    );
  }

  await new Promise((resolve) => setTimeout(resolve, 1000));
  await service1.stop();
  await service2.stop();
  await client.disconnect();
  await server1.disconnect();
  await server2.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nTest completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
