export default {
  exclude: [
    'typescript',
    'eslint',
    'unzipper',
    '@stylistic/eslint-plugin',
    '@nats-io/nats-core',
    '@nats-io/services',
    '@nats-io/transport-node',
  ],
  cooldown: '7d',
  overrides: [
    {
      include: [
        /^(@camera\.ui\/|camera\.ui$|node-av(-light|-lgpl)?$|@seydx\/|camera[-_]ui[-_]|github\.com\/(cameraui|seydx)\/)/,
      ],
      cooldown: 0,
    },
  ],
};
