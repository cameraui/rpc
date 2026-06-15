import { RPCClass } from '../src/decorators.js';
import { createRPCClient, ERROR_CODES, RPCException } from '../src/index.js';

class PerformanceTimer {
  private name: string;
  private operations: { operation: string; duration: number }[] = [];

  constructor(name: string) {
    this.name = name;
  }

  startOperation(operation: string): OperationTimer {
    return new OperationTimer(this, operation);
  }

  recordOperation(operation: string, duration: number): void {
    this.operations.push({ operation, duration });
    console.log(`  ${operation}: ${duration.toFixed(2)}ms`);
  }

  printSummary(): void {
    console.log(`\n${this.name} Performance Summary:`);
    console.log('='.repeat(50));
    const totalTime = this.operations.reduce((sum, op) => sum + op.duration, 0);

    for (const { operation, duration } of this.operations) {
      const percentage = totalTime > 0 ? (duration / totalTime) * 100 : 0;
      const padding = '.'.repeat(Math.max(40 - operation.length, 0));
      console.log(`${operation}${padding} ${duration.toFixed(2).padStart(8)}ms (${percentage.toFixed(1).padStart(5)}%)`);
    }

    console.log('='.repeat(50));
    const totalPadding = '.'.repeat(Math.max(40 - 'Total Time:'.length, 0));
    console.log(`Total Time:${totalPadding} ${totalTime.toFixed(2).padStart(8)}ms`);
    console.log();
  }
}

class OperationTimer {
  private timer: PerformanceTimer;
  private operation: string;
  private startTime = 0;

  constructor(timer: PerformanceTimer, operation: string) {
    this.timer = timer;
    this.operation = operation;
  }

  start(): void {
    this.startTime = performance.now();
  }

  end(): void {
    const duration = performance.now() - this.startTime;
    this.timer.recordOperation(this.operation, duration);
  }
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const smallData = 'This is a small test message';
const mediumData = Buffer.alloc(5 * 1024 * 1024); // 5MB
const largeData = Buffer.alloc(10 * 1024 * 1024); // 10MB

for (let i = 0; i < mediumData.length; i++) {
  mediumData[i] = i % 256;
}
for (let i = 0; i < largeData.length; i++) {
  largeData[i] = i % 256;
}

@RPCClass
class TestService {
  async echo(msg: string): Promise<string> {
    return msg;
  }

  async add(a: number, b: number): Promise<number> {
    return a + b;
  }

  async getLargeData(): Promise<Buffer> {
    return largeData;
  }

  async echoData(data: Buffer): Promise<Buffer> {
    return data;
  }

  async *generateNumbers(count: number): AsyncGenerator<number> {
    for (let i = 0; i < count; i++) {
      yield i;
    }
  }

  async errorMethod(shouldFail: boolean): Promise<string> {
    if (shouldFail) {
      throw new RPCException(ERROR_CODES.INTERNAL_ERROR, 'Test error');
    }
    return 'Success';
  }
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function testAllFeatures(): Promise<void> {
  console.log('Comprehensive RPC Performance Test\n');

  const timer = new PerformanceTimer('All-in-One Test');

  const serverOp = timer.startOperation('Server Client Creation');
  serverOp.start();
  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'test-server',
    auth: { user: 'server', password: 'server_password' },
  });
  await server.connect();
  serverOp.end();

  const clientOp = timer.startOperation('Client Creation');
  clientOp.start();
  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'test-client',
    auth: { user: 'server', password: 'server_password' },
  });
  await client.connect();
  clientOp.end();

  console.log('\n1. Native Request/Reply');
  const test1Op = timer.startOperation('Request Handler Setup & 100 calls');
  test1Op.start();

  const unsub1 = await server.onRequest('echo.*', async (data: any) => {
    return { echo: data.msg, timestamp: Date.now() };
  });
  await sleep(50); // Let subscription settle

  for (let i = 0; i < 100; i++) {
    const result = await client.request('echo.test', { msg: `Message ${i}` });
    if (result.echo !== `Message ${i}`) {
      throw new Error('Echo mismatch');
    }
  }

  unsub1();
  test1Op.end();

  console.log('\n2. Register Handlers (RPC style)');
  const test2Op = timer.startOperation('RPC Handler Setup & 100 calls');
  test2Op.start();

  const handlers = {
    echo: async (msg: string) => `Echo: ${msg}`,
    add: async (a: number, b: number) => a + b,
  };

  const unsub2 = await server.registerHandler('test', handlers);
  await sleep(50);

  const testProxy = client.createProxy<typeof handlers>('test');

  for (let i = 0; i < 50; i++) {
    const echoResult = await testProxy.echo(`Message ${i}`);
    if (echoResult !== `Echo: Message ${i}`) {
      throw new Error('Echo result mismatch');
    }

    const addResult = await testProxy.add(i, i + 1);
    if (addResult !== 2 * i + 1) {
      throw new Error('Add result mismatch');
    }
  }

  await unsub2();
  test2Op.end();

  console.log('\n3. Large Data Transfer (Auto-Chunking)');
  const test3Op = timer.startOperation('Large Data Transfer (10MB)');
  test3Op.start();

  const largeHandlers = {
    getLarge: async () => largeData,
    echoData: async (data: Buffer) => data,
  };

  const unsub3 = await server.registerHandler('data', largeHandlers);
  await sleep(50);

  const dataProxy = client.createProxy<typeof largeHandlers>('data');

  const result = await dataProxy.getLarge();
  if (result.length !== largeData.length) {
    throw new Error('Large data size mismatch');
  }

  const echoResult = await dataProxy.echoData(mediumData);
  const echoBuffer = Buffer.isBuffer(echoResult) ? echoResult : Buffer.from(echoResult);
  if (!echoBuffer.equals(mediumData)) {
    throw new Error('Echo data mismatch');
  }

  await unsub3();
  test3Op.end();

  console.log('\n4. Channel Communication');
  const test4Op = timer.startOperation('Channel Setup & 1000 messages');
  test4Op.start();

  const serverChannel = await server.channel('perf-channel');
  const clientChannel = await client.channel('perf-channel');

  const messagesReceived: any[] = [];

  serverChannel.on('message', (msg) => {
    messagesReceived.push(msg);
  });
  await sleep(50);

  for (let i = 0; i < 1000; i++) {
    await clientChannel.send({ index: i, data: `Message ${i}` });
  }

  await sleep(200);
  if (messagesReceived.length !== 1000) {
    throw new Error(`Expected 1000 messages, got ${messagesReceived.length}`);
  }

  await serverChannel.close();
  await clientChannel.close();
  test4Op.end();

  console.log('\n5. Private Channel Communication');
  const test5Op = timer.startOperation('Private Channel Setup & 100 calls');
  test5Op.start();

  const serverPrivate = await server.privateChannel('perf-private', 'test-client');

  const unsub5 = await serverPrivate.onRequest(async (data: any) => {
    return { processed: data.value * 2 };
  });
  await sleep(50);

  const clientPrivate = await client.privateChannel('perf-private', 'test-server');

  for (let i = 0; i < 100; i++) {
    const result = await clientPrivate.request({ value: i }, 5000);
    if (result.processed !== i * 2) {
      throw new Error('Private channel result mismatch');
    }
  }

  unsub5();
  await serverPrivate.close();
  await clientPrivate.close();
  test5Op.end();

  console.log('\n6. Service Creation & Discovery');
  const test6Op = timer.startOperation('Service Setup');
  test6Op.start();

  const service = await server.service.registerHandler(
    {
      name: 'perf-service',
      version: '1.0.0',
      description: 'Performance test service',
    },
    new TestService(),
  );

  await sleep(100);

  const services = server.service.getAllInfo();
  if (services.length === 0) {
    throw new Error('Service not found');
  }
  if (services[0].name !== 'perf-service') {
    throw new Error('Service name mismatch');
  }

  test6Op.end();

  console.log('\n7. Service Proxy Calls');
  const test7Op = timer.startOperation('Service Proxy Creation & 50 calls');
  test7Op.start();

  const calcService = await client.createServiceProxy<TestService>('perf-service');

  for (let i = 0; i < 50; i++) {
    const result = await calcService.add(i, i + 1);
    if (result !== 2 * i + 1) {
      throw new Error('Service add result mismatch');
    }
  }

  test7Op.end();

  console.log('\n8. Service Streaming');
  const test8Op = timer.startOperation('Stream Processing (100 items)');
  test8Op.start();

  let streamSum = 0;
  for await (const value of calcService.generateNumbers(100)) {
    streamSum += value;
  }

  const expectedSum = (99 * 100) / 2; // sum of 0 to 99
  if (streamSum !== expectedSum) {
    throw new Error(`Stream sum mismatch: ${streamSum} !== ${expectedSum}`);
  }

  test8Op.end();

  // Services don't support chunking; use an RPC handler for large data
  console.log('\n8b. Large Data via RPC Handler');
  const test8bOp = timer.startOperation('Get Large Data (10MB) via RPC');
  test8bOp.start();

  const largeDataHandlers = {
    getLargeData: async () => largeData,
  };

  const unsub8b = await server.registerHandler('largedata', largeDataHandlers);
  await sleep(50);

  const largeProxy = client.createProxy<typeof largeDataHandlers>('largedata');
  const largeResult = await largeProxy.getLargeData();
  if (largeResult.length !== largeData.length) {
    throw new Error('Large data RPC size mismatch');
  }

  await unsub8b();
  test8bOp.end();

  console.log('\n9. Concurrent Operations');
  const test9Op = timer.startOperation('Concurrent Requests (500 parallel)');
  test9Op.start();

  const unsub9 = await server.onRequest('echo.concurrent', async (data: any) => {
    return { echo: data, handled: true };
  });
  await sleep(50);

  const concurrentCall = async (i: number) => {
    return await client.request('echo.concurrent', { index: i });
  };

  const tasks = Array.from({ length: 500 }, (_, i) => concurrentCall(i));
  const results = await Promise.all(tasks);
  if (results.length !== 500) {
    throw new Error('Concurrent results count mismatch');
  }

  // Keep the handler for mixed workload test
  test9Op.end();

  console.log('\n10. Error Handling');
  const test10Op = timer.startOperation('Error Handling Test');
  test10Op.start();

  const successResult = await calcService.echo('test');
  if (successResult !== 'test') {
    throw new Error('Echo success mismatch');
  }

  try {
    await calcService.errorMethod(true);
    throw new Error('Should have raised exception');
  } catch (e: any) {
    // Services return HTTP-like error codes (500), not ERROR_CODES
    if (!(e instanceof RPCException) || (e.code !== '500' && e.code !== ERROR_CODES.INTERNAL_ERROR)) {
      throw new Error(`Unexpected error: ${e}`);
    }
  }

  test10Op.end();

  console.log('\n11. Isolated Connection Proxy');
  const test11Op = timer.startOperation('Isolated Proxy Test');
  test11Op.start();

  const handlersIsolated = {
    echo: async (msg: string) => `Echo: ${msg}`,
    add: async (a: number, b: number) => a + b,
  };

  const unsub11 = await server.registerHandler('test', handlersIsolated);
  await sleep(50);

  const isolatedProxyWithClose = client.createProxy<typeof handlersIsolated>('test', { isolatedConnection: true });

  for (let i = 0; i < 10; i++) {
    const result = await isolatedProxyWithClose.proxy.echo(`Isolated ${i}`);
    if (result !== `Echo: Isolated ${i}`) {
      throw new Error('Isolated echo mismatch');
    }
  }

  await isolatedProxyWithClose.close();
  await unsub11();
  test11Op.end();

  console.log('\n12. Mixed Workload (Simulating Real Usage)');
  const test12Op = timer.startOperation('Mixed Operations');
  test12Op.start();

  const testChannel = await client.channel('mixed-channel');

  const mixedOperation = async () => {
    const ops: Promise<any>[] = [];

    for (let i = 0; i < 10; i++) {
      ops.push(client.request('echo.concurrent', { msg: 'quick' }));

      ops.push(calcService.add(Math.floor(Math.random() * 100), Math.floor(Math.random() * 100)));

      ops.push(testChannel.send({ event: 'mixed', data: 'test' }));
    }

    // Service calls don't support auto-chunking; use echo to avoid "maximum payload exceeded"
    ops.push(calcService.echo('test data'));

    await Promise.all(ops);
  };

  const rounds: Promise<void>[] = [];
  for (let i = 0; i < 5; i++) {
    rounds.push(mixedOperation());
  }

  await Promise.all(rounds);
  await testChannel.close();
  test12Op.end();

  console.log('\nCleanup');
  const cleanupOp = timer.startOperation('Cleanup');
  cleanupOp.start();

  unsub9(); // Clean up the concurrent handler
  await service.stop();
  await client.disconnect();
  await server.disconnect();

  cleanupOp.end();

  timer.printSummary();
}

async function main(): Promise<void> {
  try {
    await testAllFeatures();
    console.log('All tests completed successfully!');
  } catch (error) {
    console.error('Test failed:', error);
    process.exit(1);
  }
}

main().catch(console.error);
