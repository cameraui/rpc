import { createRPCClient } from '../src/index.js';

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseTargets(argv: string[]): string[] {
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--targets' && i + 1 < argv.length) {
      return argv[i + 1].split(',').map((t) => t.trim());
    }
    if (argv[i].startsWith('--targets=')) {
      return argv[i]
        .slice('--targets='.length)
        .split(',')
        .map((t) => t.trim());
    }
  }
  return ['python-service'];
}

function infoMethodForTarget(target: string): string {
  const mapping: Record<string, string> = {
    'python-service': 'getPythonInfo',
    'node-service': 'getNodeInfo',
    'go-service': 'getGoInfo',
  };
  return mapping[target] || 'getInfo';
}

async function runNodeServer() {
  const targets = parseTargets(process.argv.slice(2));

  console.log('Node.js RPC Server Starting...');
  console.log(`   Targets: ${JSON.stringify(targets)}`);

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    name: 'node-server-unique',
    auth: { user: 'server', password: 'server_password' },
  });

  await server.connect();
  console.log('Node.js server connected');

  const handlers = {
    name: 'node-service',

    greet: async (name: string) => {
      console.log(`Received greet request for: ${name}`);
      return `Hello ${name} from Node.js!`;
    },

    calculate: async (a: number, b: number, operation: string) => {
      console.log(`Calculate: ${a} ${operation} ${b}`);
      switch (operation) {
        case 'add':
          return a + b;
        case 'subtract':
          return a - b;
        case 'multiply':
          return a * b;
        case 'divide':
          return b !== 0 ? a / b : 'Error: Division by zero';
        default:
          return 'Unknown operation';
      }
    },

    getNodeInfo: async () => {
      console.log('Returning node info');
      return {
        platform: 'Node.js',
        version: process.version,
        timestamp: new Date().toISOString(),
        pid: process.pid,
      };
    },

    echoData: async (data: any) => {
      console.log('Echoing data:', data);
      return data;
    },

    getLargeData: async () => {
      console.log('Creating 20MB test data...');
      const size = 20 * 1024 * 1024; // 20MB
      const buffer = Buffer.alloc(size);

      for (let i = 0; i < size; i++) {
        buffer[i] = i % 256;
      }

      console.log('Sending 20MB data to test auto-chunking...');
      return {
        type: 'large-data',
        size: size,
        data: buffer,
        checksum: calculateChecksum(buffer),
      };
    },

    verifyLargeData: async (payload: any) => {
      console.log(`Verifying received data: ${(payload.data.length / 1024 / 1024).toFixed(2)}MB`);

      const buffer = Buffer.from(payload.data);
      const checksum = calculateChecksum(buffer);
      const valid = checksum === payload.checksum && buffer.length === payload.size;

      console.log(`Verification: ${valid ? 'PASSED' : 'FAILED'}`);
      return { valid, receivedSize: buffer.length, checksumMatch: checksum === payload.checksum };
    },

    onStatusUpdates: async (prefix: string, callback: (update: any) => void | Promise<void>) => {
      console.log(`New callback subscriber for prefix '${prefix}'`);
      for (let i = 0; i < 3; i++) {
        await callback({ source: 'node', prefix, index: i, time: new Date().toISOString() });
        await sleep(50);
      }
      return () => {
        console.log(`Callback cleanup for prefix '${prefix}'`);
      };
    },
  };

  const unsub = await server.registerHandler('node-service', handlers);
  console.log('Node.js handlers registered');

  const channel = await server.channel('cross-language-chat');

  channel.on('message', async (msg) => {
    console.log('Channel received:', msg);

    // Respond to initial messages from any other service, not responses
    if (msg.from !== 'node' && msg.type !== 'response') {
      await channel.send({
        from: 'node',
        type: 'response',
        original: msg,
        message: `Node.js received: "${msg.message}"`,
      });
    }
  });

  console.log('Node.js channel ready');

  // Wait for other services to set up
  await sleep(3000);

  console.log('\nNode.js calling target services...\n');

  let failures = 0;
  for (const target of targets) {
    console.log(`--- Calling ${target} ---`);

    const proxy = server.createProxy<{
      name: Promise<string>;
      setName: (name: string) => Promise<void>;
      greet: (name: string) => Promise<string>;
      calculate: (a: number, b: number, operation: string) => Promise<number | string>;
      getPythonInfo: () => Promise<any>;
      getNodeInfo: () => Promise<any>;
      getGoInfo: () => Promise<any>;
      echoData: (data: any) => Promise<any>;
      getLargeData: () => Promise<any>;
      verifyLargeData: (payload: any) => Promise<any>;
      onStatusUpdates: (prefix: string, callback: (value: any) => void) => Promise<() => void>;
    }>(target);

    try {
      const serviceName = await proxy.name;
      console.log(`${target} service name:`, serviceName);

      const greeting = await proxy.greet('Node.js');
      console.log(`${target} greeting:`, greeting);

      const sum = await proxy.calculate(10, 5, 'add');
      console.log(`${target} calculation (10 + 5):`, sum);

      // Dynamic method name per target
      const infoMethod = infoMethodForTarget(target);
      const info = await (proxy as any)[infoMethod]();
      console.log(`${target} info:`, info);

      const complexData = {
        numbers: [1, 2, 3, 4, 5],
        nested: { foo: 'bar', baz: 42 },
        binary: Buffer.from('Hello from Node.js'),
        unicode: '你好世界 🌍',
      };
      const echoed = await proxy.echoData(complexData);
      console.log(`${target} echoed complex data correctly:`, echoed.nested?.foo === 'bar' && echoed.unicode === '你好世界 🌍');

      await channel.send({
        from: 'node',
        type: 'greeting',
        message: `Hello ${target}, this is Node.js speaking!`,
      });

      console.log(`\nTesting 20MB data transfer with ${target}...`);

      const largeData = await proxy.getLargeData();
      console.log(`Received ${(largeData.data.length / 1024 / 1024).toFixed(2)}MB from ${target}`);

      const receivedBuffer = Buffer.from(largeData.data);
      const checksum = calculateChecksum(receivedBuffer);
      console.log(`Data integrity check: ${checksum === largeData.checksum ? 'PASSED' : 'FAILED'}`);
      if (checksum !== largeData.checksum) failures++;

      const testBuffer = Buffer.alloc(20 * 1024 * 1024);
      for (let i = 0; i < testBuffer.length; i++) {
        testBuffer[i] = i % 256;
      }

      const verifyResult = await proxy.verifyLargeData({
        type: 'node-large-data',
        size: testBuffer.length,
        data: testBuffer,
        checksum: calculateChecksum(testBuffer),
      });
      console.log(`${target} verification of our 20MB data: ${verifyResult.valid ? 'PASSED' : 'FAILED'}`);
      if (!verifyResult.valid) failures++;

      console.log(`Testing callback subscription with ${target}...`);
      let cbCount = 0;
      let resolveCb: () => void;
      const cbDone = new Promise<void>((resolve) => {
        resolveCb = resolve;
      });
      const cbUnsub = await proxy.onStatusUpdates(`node-to-${target}`, (value: any) => {
        cbCount++;
        console.log(`Callback from ${target}: source=${value.source} index=${value.index}`);
        if (cbCount >= 3) resolveCb();
      });
      await Promise.race([cbDone, sleep(5000)]);
      cbUnsub();
      console.log(`Callback subscription test with ${target}: ${cbCount} events received`);
      if (cbCount < 3) failures++;
    } catch (error) {
      console.error(`Error calling ${target}:`, error);
      failures++;
    }

    console.log();
  }

  console.log('Node.js server running... Press Ctrl+C to stop\n');

  const shutdown = async () => {
    console.log('\nShutting down Node.js server...');
    await unsub();
    await channel.close();
    await server.disconnect();
    if (failures > 0) {
      console.error(`\nNode.js cross-language test FAILED (${failures} error(s))`);
      process.exit(1);
    }
    console.log('\nNode.js cross-language test passed');
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function calculateChecksum(buffer: Buffer): number {
  let sum = 0;
  for (let i = 0; i < buffer.length; i++) {
    sum = (sum + buffer[i]) % 0xffffffff;
  }
  return sum;
}

runNodeServer().catch(console.error);
