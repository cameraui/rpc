import { createRPCClient } from '../src/index.js';

import type { RPCClient } from '../src/index.js';

const buffer3MB = Buffer.alloc(3 * 1024 * 1024, 'x');

async function privateChannelExample() {
  console.log('Private Channel Example\n');

  const alice = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'alice',
  });

  const bob = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'bob',
  });

  const charlie = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'charlie',
  });

  await alice.connect();
  await bob.connect();
  await charlie.connect();

  console.log('All clients connected\n');

  let start = performance.now();
  const aliceChannel = await alice.privateChannel('secret-chat', 'bob');
  const aliceChannelTime = performance.now() - start;

  start = performance.now();
  const bobChannel = await bob.privateChannel('secret-chat', 'alice');
  const bobChannelTime = performance.now() - start;

  // Charlie joins the same channel ID but won't receive anything
  start = performance.now();
  const charlieChannel = await charlie.privateChannel('secret-chat', 'alice');
  const charlieChannelTime = performance.now() - start;

  console.log(`Private channels created (Alice: ${aliceChannelTime.toFixed(1)}ms, Bob: ${bobChannelTime.toFixed(1)}ms, Charlie: ${charlieChannelTime.toFixed(1)}ms)\n`);

  aliceChannel.on('message', (data) => {
    if (data.file) {
      data.file = `<${data.file.length} bytes>`;
    }

    console.log('[Alice received]:', data);
  });

  bobChannel.on('message', (data) => {
    if (data.file) {
      data.file = `<${data.file.length} bytes>`;
    }

    console.log('[Bob received]:', data);
  });

  charlieChannel.on('message', (data) => {
    if (data.file) {
      data.file = `<${data.file.length} bytes>`;
    }
    console.log('[Charlie received]:', data); // Should not receive anything
  });

  console.log('--- Private Messages ---');
  let msgStart = performance.now();
  await aliceChannel.send({ from: 'Alice', text: 'Hi Bob, this is private!' });
  const aliceSendTime = performance.now() - msgStart;

  msgStart = performance.now();
  await bobChannel.send({ from: 'Bob', text: 'Hi Alice, got your private message!' });
  const bobSendTime = performance.now() - msgStart;

  // Charlie's send should not be received by Alice or Bob
  msgStart = performance.now();
  await charlieChannel.send({ from: 'Charlie', text: 'Can anyone hear me?' });
  const charlieSendTime = performance.now() - msgStart;

  await new Promise((resolve) => setTimeout(resolve, 100));

  console.log(`\nMessage send times: Alice: ${aliceSendTime.toFixed(1)}ms, Bob: ${bobSendTime.toFixed(1)}ms, Charlie: ${charlieSendTime.toFixed(1)}ms`);

  console.log('\n--- Channel Info ---');
  console.log(`Alice connected to: ${aliceChannel.remoteId}`);
  console.log(`Bob connected to: ${bobChannel.remoteId}`);
  console.log(`Charlie connected to: ${charlieChannel.remoteId ?? 'nobody'}`);

  await aliceChannel.close();
  await bobChannel.close();
  await charlieChannel.close();

  await alice.disconnect();
  await bob.disconnect();
  await charlie.disconnect();

  console.log('\nPrivate channel example completed!');
}

async function directMessagingExample() {
  console.log('\n\nDirect Messaging System\n');

  class User {
    public channels = new Map<string, any>();

    constructor(
      public client: RPCClient,
      public name: string,
    ) {}

    async connect() {
      await this.client.connect();
    }

    async sendDM(recipient: string, message: string) {
      const channelId = [this.name, recipient].sort().join('-');

      let channel = this.channels.get(recipient);
      if (!channel) {
        channel = await this.client.privateChannel(channelId, recipient);
        this.channels.set(recipient, channel);

        channel.on('message', (data: any) => {
          console.log(`[${this.name}] DM from ${data.from}: ${data.message}`);
        });
      }

      await channel.send({ from: this.name, message });
    }

    async listenForDMs() {}

    async disconnect() {
      for (const channel of this.channels.values()) {
        await channel.close();
      }
      await this.client.disconnect();
    }
  }

  const alice = new User(
    createRPCClient({
      servers: ['nats://localhost:4222'],
      auth: { user: 'server', password: 'server_password' },
      name: 'alice',
    }),
    'alice',
  );

  const bob = new User(
    createRPCClient({
      servers: ['nats://localhost:4222'],
      auth: { user: 'server', password: 'server_password' },
      name: 'bob',
    }),
    'bob',
  );

  const charlie = new User(
    createRPCClient({
      servers: ['nats://localhost:4222'],
      auth: { user: 'server', password: 'server_password' },
      name: 'charlie',
    }),
    'charlie',
  );

  await alice.connect();
  await bob.connect();
  await charlie.connect();

  const bobFromAlice = await bob.client.privateChannel('alice-bob', 'alice');
  bobFromAlice.on('message', (data: any) => {
    if ('message' in data) {
      console.log(`[${bob.name}] DM from ${data.from}: ${data.message}`);
    } else if ('type' in data && data.type === 'file') {
      console.log(`[${bob.name}] Received file from ${data.from}: ${data.filename} (${data.size} bytes)`);
    }
  });

  const charlieFromBob = await charlie.client.privateChannel('bob-charlie', 'bob');
  charlieFromBob.on('message', (data: any) => {
    console.log(`[${charlie.name}] DM from ${data.from}: ${data.message}`);
  });

  console.log('--- Direct Messages ---');

  let dmStart = performance.now();
  await alice.sendDM('bob', 'Hey Bob, are you free for lunch?');
  const aliceDmTime = performance.now() - dmStart;

  dmStart = performance.now();
  await bob.sendDM('charlie', 'Charlie, did you finish the report?');
  const bobDmTime = performance.now() - dmStart;

  // Charlie and Alice have no channel setup, so Alice won't receive it
  dmStart = performance.now();
  await charlie.sendDM('alice', 'Alice, I need your help!');
  const charlieDmTime = performance.now() - dmStart;

  console.log(`\nDM send times: Alice: ${aliceDmTime.toFixed(1)}ms, Bob: ${bobDmTime.toFixed(1)}ms, Charlie: ${charlieDmTime.toFixed(1)}ms`);

  await new Promise((resolve) => setTimeout(resolve, 200));

  console.log('\n--- Private File Transfer ---');
  const largeFile = {
    from: 'alice',
    type: 'file',
    filename: 'confidential.pdf',
    size: 1024 * 1024 * 3,
    data: buffer3MB,
  };

  await alice.sendDM('bob', 'Sending you the confidential file...');
  const channel = alice.channels.get('bob');
  if (channel) {
    const fileStart = performance.now();
    await channel.send(largeFile);
    const fileTime = performance.now() - fileStart;
    const throughput = (3 * 1024 * 1024) / ((fileTime / 1000) * 1024 * 1024);
    console.log(`\nFile transfer completed: 3MB in ${fileTime.toFixed(1)}ms (${throughput.toFixed(1)} MB/s)`);
  }

  await new Promise((resolve) => setTimeout(resolve, 100));

  await alice.disconnect();
  await bob.disconnect();
  await charlie.disconnect();

  console.log('\nDirect messaging example completed!');
}

async function main() {
  const totalStart = performance.now();

  try {
    await privateChannelExample();
    await directMessagingExample();
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
