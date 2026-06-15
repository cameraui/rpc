import { createRPCClient } from '../src/index.js';

import type { RPCClient } from '../src/index.js';

const largeBuffer = Buffer.alloc(2 * 1024 * 1024).fill('x'); // 2MB

async function channelExample() {
  console.log('Channel Communication Example\n');

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

  console.log('Connected both clients\n');

  const channelA = await clientA.channel('chat-room-123');
  const channelB = await clientB.channel('chat-room-123');

  console.log('Created channels on both sides\n');

  channelA.on('message', (data) => {
    if (data.content) {
      data.content = `<${data.content.length} bytes>`;
    }

    console.log('[Client A received]:', data);
  });

  channelA.on('close', () => {
    console.log('[Client A] Channel closed');
  });

  channelB.on('message', (data) => {
    if (data.content) {
      data.content = `<${data.content.length} bytes>`;
    }

    console.log('[Client B received]:', data);
  });

  channelB.on('close', () => {
    console.log('[Client B] Channel closed');
  });

  console.log('--- Sending messages ---');

  const smallStart = performance.now();
  await channelA.send({ from: 'A', message: 'Hello from client A!' });
  await channelB.send({ from: 'B', message: 'Hi from client B!' });
  await channelA.send({
    type: 'user-info',
    user: { id: 1, name: 'Alice', status: 'online' },
  });
  const smallTime = performance.now() - smallStart;
  console.log(`Small messages sent in ${smallTime.toFixed(1)}ms`);

  const largeData = {
    from: 'A',
    type: 'file-transfer',
    filename: 'large-dataset.json',
    content: largeBuffer,
  };

  console.log('\n--- Sending large data (2MB) ---');
  const largeStart = performance.now();
  await channelA.send(largeData);
  const largeTime = performance.now() - largeStart;
  console.log(`Large data (2MB) sent in ${largeTime.toFixed(1)}ms (${(2000 / largeTime).toFixed(2)} MB/s)`);

  channelA.on('error', (error) => {
    console.error('[Client A] Error:', error.message);
  });

  await new Promise((resolve) => setTimeout(resolve, 100));

  console.log('\n--- Closing channel from Client A ---');
  await channelA.close();

  try {
    await channelA.send({ message: 'This should fail' });
  } catch (error: any) {
    console.log('[Client A] Expected error:', error.message);
  }

  await clientA.disconnect();
  await clientB.disconnect();

  console.log('\nChannel communication example completed!');
}

async function chatRoomExample() {
  console.log('\n\nMulti-Party Chat Room Example\n');

  const users = ['Alice', 'Bob', 'Charlie'];
  const clients: RPCClient[] = [];
  const channels: any[] = [];
  const roomId = 'team-standup';

  for (const user of users) {
    const client = createRPCClient({
      servers: ['nats://localhost:4222'],
      auth: { user: 'server', password: 'server_password' },
      name: `user-${user.toLowerCase()}`,
    });
    await client.connect();
    clients.push(client);

    const channel = await client.channel(roomId);
    channels.push({ user, channel });

    channel.on('message', (data: any) => {
      if (data.from !== user) {
        console.log(`[${user}] ${data.from}: ${data.message}`);
      }
    });
  }

  console.log('All users joined the chat room\n');

  await channels[0].channel.send({ from: 'Alice', message: 'Good morning team!' });
  await channels[1].channel.send({ from: 'Bob', message: 'Hi Alice! Ready for standup?' });
  await channels[2].channel.send({ from: 'Charlie', message: 'Hey everyone!' });

  await new Promise((resolve) => setTimeout(resolve, 100));

  console.log('\n--- File sharing ---');
  await channels[0].channel.send({
    from: 'Alice',
    type: 'file-share',
    message: "Sharing today's agenda",
    file: {
      name: 'standup-agenda.md',
      size: 1024,
      content: "# Today's Standup\n1. Updates\n2. Blockers\n3. Plans",
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 100));

  for (const { channel } of channels) {
    await channel.close();
  }
  for (const client of clients) {
    await client.disconnect();
  }

  console.log('\nChat room example completed!');
}

async function main() {
  const totalStart = performance.now();

  try {
    await channelExample();
    await chatRoomExample();
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
