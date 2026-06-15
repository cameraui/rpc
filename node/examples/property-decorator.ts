import { RPCClass, RPCProperty } from '../src/decorators.js';
import { createRPCClient } from '../src/index.js';

@RPCClass
class ConfigService {
  @RPCProperty
  public name: string;

  @RPCProperty
  public version: string;

  @RPCProperty
  public maxConnections: number;

  constructor() {
    this.name = 'Config Service';
    this.version = '1.0.0';
    this.maxConnections = 100;
  }
}

async function main() {
  const totalStart = performance.now();
  console.log('RPCProperty Decorator Example\n');

  const server = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'property-server',
  });

  await server.connect();
  console.log('Server connected');

  const unsub = await server.registerHandler('config', new ConfigService());
  console.log('Service registered\n');

  const client = createRPCClient({
    servers: ['nats://localhost:4222'],
    auth: { user: 'server', password: 'server_password' },
    name: 'property-client',
  });

  await client.connect();
  console.log('Client connected\n');

  const config = client.createProxy<{
    name: Promise<string>;
    version: Promise<string>;
    maxConnections: Promise<number>;
    setName(name: string): Promise<void>;
  }>('config');

  console.log('--- Testing Property Getters ---');
  const name = await config.name;
  console.log(`Name: ${name}`);

  const version = await config.version;
  console.log(`Version: ${version}`);

  const maxConnections = await config.maxConnections;
  console.log(`Max Connections: ${maxConnections}`);

  console.log('\n--- Updating Name ---');
  await config.setName('Updated Config Service');

  const newName = await config.name;
  console.log(`New Name: ${newName}`);

  console.log('\nCleaning up...');
  await unsub();
  await client.disconnect();
  await server.disconnect();

  const totalElapsed = performance.now() - totalStart;
  console.log(`\nAll tests completed! Total time: ${totalElapsed.toFixed(1)}ms`);
}

main().catch(console.error);
