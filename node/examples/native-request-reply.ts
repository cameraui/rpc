import { createRPCClient } from '../src/index.js';

async function main() {
  const totalStart = performance.now();

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'server',
  });

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'client',
  });

  await server.connect();
  await client.connect();

  console.log('Connected to NATS\n');

  console.log('Testing Native NATS Request/Reply\n');

  const unsubEcho = await server.onRequest<any, { echo: any; timestamp: number }>('echo', async (data) => {
    console.log(`Received echo request: ${JSON.stringify(data)}`);
    return { echo: data, timestamp: performance.now() };
  });

  try {
    console.log('Sending echo request...');
    const start = performance.now();
    const response = await client.request<{ message: string }, { echo: string; timestamp: number }>('echo', { message: 'Hello NATS!' });
    const elapsed = performance.now() - start;
    console.log(`Got response: ${JSON.stringify(response)} (${elapsed.toFixed(1)}ms)\n`);
  } catch (error) {
    console.error('Request failed:', error);
  }

  console.log('Testing Math Service\n');

  const unsubMath = await server.onRequest('math.*', async (data) => {
    const operation = data?.operation;

    console.log(`Math operation: ${operation}, data: ${JSON.stringify(data)}`);

    switch (operation) {
      case 'add':
        return { result: data.a + data.b };
      case 'multiply':
        return { result: data.a * data.b };
      case 'divide':
        if (data.b === 0) throw new Error('Division by zero');
        return { result: data.a / data.b };
      default:
        throw new Error(`Unknown operation: ${operation}`);
    }
  });

  const mathTests = [
    { subject: 'math.add', data: { a: 5, b: 3, operation: 'add' } },
    { subject: 'math.multiply', data: { a: 4, b: 7, operation: 'multiply' } },
    { subject: 'math.divide', data: { a: 20, b: 4, operation: 'divide' } },
    { subject: 'math.divide', data: { a: 10, b: 0, operation: 'divide' } }, // Error case
  ];

  for (const test of mathTests) {
    try {
      console.log(`Requesting ${test.subject} with ${JSON.stringify(test.data)}`);
      const start = performance.now();
      const result = await client.request(test.subject, test.data);
      const elapsed = performance.now() - start;
      console.log(`Result: ${JSON.stringify(result)} (${elapsed.toFixed(1)}ms)`);
    } catch (error: any) {
      console.log(`Error: ${error.message}`);
    }
  }

  console.log('\nTesting Large Data Request/Reply\n');

  const unsubData = await server.onRequest('data.large', async (data) => {
    console.log(`Got request for ${data.size}MB of data`);
    const buffer = Buffer.alloc(data.size * 1024 * 1024, 'x');
    return {
      data: buffer,
      size: buffer.length,
      checksum: buffer.length, // Simplified checksum
    };
  });

  try {
    console.log('Requesting 500KB of data...');
    const start = performance.now();
    const response = await client.request('data.large', { size: 0.5 }, { timeout: 10000 });
    const elapsed = performance.now() - start;
    const throughput = response.size / 1024 / 1024 / (elapsed / 1000);
    console.log(`Got ${(response.size / 1024).toFixed(1)}KB in ${elapsed.toFixed(1)}ms (${throughput.toFixed(1)} MB/s)\n`);
  } catch (error) {
    console.error('Large data request failed:', error);
  }

  console.log('Testing Concurrent Requests\n');

  let requestCount = 0;
  const unsubConcurrent = await server.onRequest('concurrent.test', async (data) => {
    const id = ++requestCount;
    const delay = Math.random() * 200;
    console.log(`Processing request ${id}, delay: ${delay.toFixed(0)}ms`);

    await new Promise((resolve) => setTimeout(resolve, delay));

    return {
      requestId: id,
      input: data,
      processingTime: delay.toFixed(0) + 'ms',
    };
  });

  const promises: Promise<void>[] = [];
  console.log('Sending 5 concurrent requests...');
  const concurrentStart = performance.now();
  for (let i = 0; i < 5; i++) {
    promises.push(
      (async () => {
        const start = performance.now();
        const res = await client.request('concurrent.test', { index: i });
        const elapsed = performance.now() - start;
        console.log(`Response ${i}: ${JSON.stringify(res)} (${elapsed.toFixed(1)}ms)`);
        return res;
      })(),
    );
  }

  await Promise.all(promises);
  const concurrentElapsed = performance.now() - concurrentStart;
  console.log(`\nAll concurrent requests completed in ${concurrentElapsed.toFixed(1)}ms!\n`);

  console.log('Testing No Responders\n');

  console.log('Requesting non-existent service...');
  const noRespStart = performance.now();
  try {
    await client.request('does.not.exist', { test: true }, { timeout: 1000 });
  } catch (error: any) {
    const elapsed = performance.now() - noRespStart;
    console.log(`Expected error: ${error.message} (${elapsed.toFixed(1)}ms)\n`);
  }

  console.log('Testing Timeout\n');

  const unsubSlow = await server.onRequest('slow.service', async () => {
    console.log('Slow service - waiting 3 seconds...');
    await new Promise((resolve) => setTimeout(resolve, 3000));
    return { done: true };
  });

  console.log('Requesting slow service with 1s timeout...');
  const timeoutStart = performance.now();
  try {
    await client.request('slow.service', { test: true }, { timeout: 1000 });
  } catch (error: any) {
    const elapsed = performance.now() - timeoutStart;
    console.log(`Expected timeout: ${error.message} (${elapsed.toFixed(1)}ms)\n`);
  }

  unsubEcho();
  unsubMath();
  unsubData();
  unsubConcurrent();
  unsubSlow();

  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`All tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
