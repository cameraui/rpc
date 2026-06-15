import { createRPCClient } from '../src/index.js';

const largeBuffer = Buffer.alloc(5 * 1024 * 1024).fill('x'); // 5MB

async function main() {
  const totalStart = performance.now();

  const clientA = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'client-a',
  });

  const clientB = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'client-b',
  });

  await clientA.connect();
  await clientB.connect();

  console.log('Clients connected\n');

  console.log('=== Testing Public Channel with Native Request/Reply ===\n');

  const channelA = await clientA.channel('test-channel');
  const channelB = await clientB.channel('test-channel');

  const unsubB = await channelB.onRequest(async (data) => {
    console.log(`[Client B] Received request: ${JSON.stringify(data)}`);

    await new Promise((resolve) => setTimeout(resolve, 100));

    const result = { answer: data.question?.toUpperCase() ?? 'NO QUESTION', processed: true };
    console.log(`[Client B] Sending reply: ${JSON.stringify(result)}`);
    return result;
  });

  try {
    console.log('[Client A] Sending request...');
    const reqStart = performance.now();
    const response = await channelA.request({ question: 'hello world' }, 2000);
    const reqTime = performance.now() - reqStart;
    console.log(`[Client A] Got response: ${JSON.stringify(response)}`);
    console.log(`[Client A] Request took ${reqTime.toFixed(1)}ms\n`);
  } catch (error) {
    console.error('[Client A] Request failed:', error);
  }

  console.log('=== Testing Private Channel with Native Request/Reply ===\n');

  const privateA = await clientA.privateChannel('private-chat', 'client-b');
  const privateB = await clientB.privateChannel('private-chat', 'client-a');

  const unsubPrivB = await privateB.onRequest(async (data) => {
    console.log(`[Client B Private] Received request: ${JSON.stringify(data)}`);

    if (data.type === 'calculation') {
      return { result: data.a + data.b };
    } else if (data.type === 'error-test') {
      throw new Error('Simulated error');
    }

    return { echo: data };
  });

  try {
    console.log('[Client A Private] Sending calculation request...');
    const calcStart = performance.now();
    const result = await privateA.request({ type: 'calculation', a: 5, b: 3 });
    const calcTime = performance.now() - calcStart;
    console.log(`[Client A Private] Got result: ${JSON.stringify(result)}`);
    console.log(`[Client A Private] Calculation request took ${calcTime.toFixed(1)}ms\n`);
  } catch (error) {
    console.error('[Client A Private] Request failed:', error);
  }

  try {
    console.log('[Client A Private] Sending error test request...');
    await privateA.request({ type: 'error-test' });
  } catch (error: any) {
    console.log(`[Client A Private] Got expected error: ${error.message}\n`);
  }

  console.log('=== Testing Concurrent Channel Requests ===\n');

  // Unsubscribe the previous handler to avoid conflicts
  unsubB();

  let requestCount = 0;
  const unsubConcurrent = await channelB.onRequest(async (data) => {
    const reqNum = ++requestCount;
    console.log(`[Client B] Processing request ${reqNum}...`);

    const delay = Math.random() * 200;
    await new Promise((resolve) => setTimeout(resolve, delay));

    return {
      original: data,
      requestNumber: reqNum,
      processingTime: delay.toFixed(0) + 'ms',
    };
  });

  const promises: Promise<void>[] = [];
  for (let i = 0; i < 5; i++) {
    promises.push(
      channelA.request({ index: i, timestamp: performance.now() }).then((response) => {
        console.log(`[Client A] Response for request ${i}: ${JSON.stringify(response)}`);
        return response;
      }),
    );
  }

  await Promise.all(promises);
  console.log('\nAll concurrent requests completed\n');

  console.log('=== Testing Channel Request/Reply with Large Data ===\n');

  unsubPrivB();

  const unsubLarge = await privateB.onRequest(async (data) => {
    if (data.type === 'large-data') {
      console.log('[Client B] Processing large data request...');

      console.log(`[Client B] Sending large response (${(largeBuffer.length / 1024 / 1024).toFixed(1)}MB)`);

      return {
        echo: data.message,
        data: largeBuffer,
        size: largeBuffer.length,
      };
    }
  });

  try {
    console.log('[Client A] Sending large data request...');
    const start = performance.now();
    const response = await privateA.request(
      {
        type: 'large-data',
        message: 'Please send me large data',
      },
      10000,
    );
    const elapsed = performance.now() - start;

    console.log(`[Client A] Got large response: size=${(response.data.length / 1024 / 1024).toFixed(1)}MB, time=${elapsed.toFixed(1)}ms`);
    console.log(`[Client A] Throughput: ${(5 / (elapsed / 1000)).toFixed(2)} MB/s\n`);
  } catch (error) {
    console.error('[Client A] Large data request failed:', error);
  }

  console.log('=== Testing Mixed Messages and Requests ===\n');

  channelB.on('message', (data) => {
    console.log(`[Client B] Regular message: ${JSON.stringify(data)}`);
  });

  await channelA.send({ type: 'regular', content: 'This is a regular message' });

  try {
    const response = await channelA.request({ type: 'request', content: 'This is a request' });
    console.log(`[Client A] Request response: ${JSON.stringify(response)}\n`);
  } catch (error) {
    console.error('[Client A] Request failed:', error);
  }

  unsubB();
  unsubPrivB();
  unsubConcurrent();
  unsubLarge();

  await channelA.close();
  await channelB.close();
  await privateA.close();
  await privateB.close();
  await clientA.disconnect();
  await clientB.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`Test completed. Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
