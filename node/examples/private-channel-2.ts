import { createRPCClient } from '../src/index.js';

const buffer10MB = Buffer.alloc(10 * 1024 * 1024, 'x');

async function main() {
  const totalStart = performance.now();
  let isConnected = false;

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

  console.log('Clients connected');

  isConnected = true;

  let start = performance.now();
  const clientAChannel = await clientA.privateChannel('secret-chat', 'client-b', {
    isolatedConnection: true,
  });
  const channelATime = performance.now() - start;

  start = performance.now();
  const clientBChannel = await clientB.privateChannel('secret-chat', 'client-a', {
    isolatedConnection: true,
  });
  const channelBTime = performance.now() - start;

  console.log(`Private channels created (A: ${channelATime.toFixed(1)}ms, B: ${channelBTime.toFixed(1)}ms)`);

  clientAChannel.on('message', (data) => {
    const msgInfo: any = { ...data };
    delete msgInfo.file;
    if (data.file) {
      msgInfo.file_size = data.file.length;
    }
    console.log('[Client A received]:', msgInfo);
  });

  clientBChannel.on('message', (data) => {
    const msgInfo: any = { ...data };
    delete msgInfo.file;
    if (data.file) {
      msgInfo.file_size = data.file.length;
    }
    console.log('[Client B received]:', msgInfo);
  });

  const testInfiniteMessage = async () => {
    let count = 0;
    let currentClient = clientAChannel;
    const file = buffer10MB;
    const sendTimes: number[] = [];
    while (isConnected) {
      try {
        const from = currentClient === clientAChannel ? 'Client A' : 'Client B';
        const sendStart = performance.now();
        await currentClient.send({ from, text: `Message ${count}`, date: new Date().toISOString(), file });
        const sendTime = performance.now() - sendStart;
        sendTimes.push(sendTime);
        const throughput = (10 * 1024 * 1024) / ((sendTime / 1000) * 1024 * 1024);
        console.log(`[${from}] Sent 10MB message ${count} in ${sendTime.toFixed(1)}ms (${throughput.toFixed(1)} MB/s)`);
        count++;
      } catch (err) {
        if (!isConnected) {
          break;
        }

        console.error('Error sending message:', err);
      }

      if (isConnected) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        currentClient = currentClient === clientAChannel ? clientBChannel : clientAChannel;
      }
    }
  };

  testInfiniteMessage().catch((err) => {
    console.error('Error in infinite message test:', err);
  });

  await new Promise((resolve) => setTimeout(resolve, 10000));
  console.log('\nDisconnecting clients...');
  isConnected = false;
  await clientAChannel.close();
  await clientBChannel.close();
  await clientA.disconnect();
  await clientB.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
