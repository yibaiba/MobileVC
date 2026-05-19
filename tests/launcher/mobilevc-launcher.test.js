const test = require('node:test');
const assert = require('node:assert/strict');
const EventEmitter = require('node:events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('net');

const {
  isPortOccupied,
  assertValidRelayURL,
  buildLaunchUrl,
  buildPublicAccessConfig,
  buildRelayPairingUri,
  formatLaunchUrlForDisplay,
  normalizeOrigin,
  parseInvocation,
  readRelayPairingEventFile,
  removeRelayPairingEventFile,
  resolveBinaryInfo,
} = require('../../bin/mobilevc.js');

test('parseInvocation treats bare mobilevc as guided start', () => {
  const invocation = parseInvocation([]);
  assert.equal(invocation.command, 'start');
  assert.equal(invocation.options.guided, true);
});

test('parseInvocation keeps explicit start non-guided', () => {
  const invocation = parseInvocation(['start']);
  assert.equal(invocation.command, 'start');
  assert.equal(invocation.options.guided, false);
});

test('parseInvocation supports public origin shorthand', () => {
  const invocation = parseInvocation(['start', '--public', '--origin', 'https://example.test']);
  assert.equal(invocation.command, 'start');
  assert.equal(invocation.options.public, true);
  assert.deepEqual(invocation.options.origins, ['https://example.test']);
});

test('parseInvocation supports public command relay shorthand', () => {
  const invocation = parseInvocation(['public', 'wss://relay.example.test']);
  assert.equal(invocation.command, 'public');
  assert.equal(invocation.options.public, true);
  assert.equal(invocation.options.relay, 'wss://relay.example.test');
});

test('normalizeOrigin strips default ports and rejects paths', () => {
  assert.equal(normalizeOrigin('https://example.test:443/'), 'https://example.test');
  assert.equal(normalizeOrigin('http://127.0.0.1:80'), 'http://127.0.0.1');
  assert.equal(normalizeOrigin('http://127.0.0.1:8001'), 'http://127.0.0.1:8001');
  assert.throws(() => normalizeOrigin('https://example.test/path'), /invalid origin/);
});

test('assertValidRelayURL rejects http and public ws relay urls', () => {
  assert.doesNotThrow(() => assertValidRelayURL('wss://relay.example.test'));
  assert.doesNotThrow(() => assertValidRelayURL('ws://127.0.0.1:9000'));
  assert.doesNotThrow(() => assertValidRelayURL('ws://192.168.1.10:9000'));
  assert.throws(() => assertValidRelayURL('https://relay.example.test'), /ws:\/\/ or wss:\/\//);
  assert.throws(() => assertValidRelayURL('ws://relay.example.test'), /loopback or LAN/);
});

test('buildRelayPairingUri does not include direct backend token', () => {
  const uri = buildRelayPairingUri({
    relayUrl: 'wss://relay.example.test',
    sessionId: 'rs_test',
    pairingSecret: 'pair_secret',
    expiresAt: 1760000000,
  });

  const parsed = new URL(uri);
  assert.equal(parsed.protocol, 'mobilevc:');
  assert.equal(parsed.hostname, 'relay');
  assert.equal(parsed.searchParams.get('relay'), 'wss://relay.example.test');
  assert.equal(parsed.searchParams.get('session'), 'rs_test');
  assert.equal(parsed.searchParams.get('secret'), 'pair_secret');
  assert.equal(parsed.searchParams.has('token'), false);
});

test('relay pairing event file is read locally and removable', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mobilevc-relay-test-'));
  const eventPath = path.join(dir, 'pairing.json');
  const event = {
    type: 'mobilevc.relay.pairing_ready',
    relayUrl: 'wss://relay.example.test',
    sessionId: 'rs_test',
    pairingSecret: 'pair_secret',
    expiresAt: 1760000000,
  };
  fs.writeFileSync(eventPath, `${JSON.stringify(event)}\n`, { mode: 0o600 });

  assert.deepEqual(readRelayPairingEventFile(eventPath), event);
  removeRelayPairingEventFile(eventPath);
  assert.equal(fs.existsSync(eventPath), false);
});

test('isPortOccupied falls back from wildcard probe to IPv4 probe', async () => {
  const originalCreateServer = net.createServer;
  const listenCalls = [];
  let attempts = 0;

  net.createServer = () => {
    const server = new EventEmitter();
    server.listen = (options) => {
      listenCalls.push(options);
      attempts += 1;
      queueMicrotask(() => {
        const code = attempts === 1 ? 'EAFNOSUPPORT' : 'EADDRINUSE';
        server.emit('error', Object.assign(new Error(code), { code }));
      });
    };
    server.close = (callback) => {
      if (callback) {
        callback();
      }
    };
    return server;
  };

  try {
    assert.equal(await isPortOccupied(8123), true);
    assert.deepEqual(listenCalls, [
      { port: 8123 },
      { port: 8123, host: '0.0.0.0' },
    ]);
  } finally {
    net.createServer = originalCreateServer;
  }
});

test('resolveBinaryInfo can fall back to bundled package paths in repo', () => {
  const info = resolveBinaryInfo('darwin-arm64');
  assert.ok(info.binaryPath.endsWith('/packages/server-darwin-arm64/bin/mobilevc-server'));
});

test('formatLaunchUrlForDisplay redacts token only when requested', () => {
  const url = buildLaunchUrl('127.0.0.1', '8001', 'secret-token', '/tmp/work');

  assert.equal(formatLaunchUrlForDisplay(url, false), url);
  const redacted = formatLaunchUrlForDisplay(url, true);
  assert.equal(redacted.includes('secret-token'), false);
  assert.equal(redacted.includes('token=%3Credacted%3E'), true);
  assert.equal(redacted.includes('cwd=%2Ftmp%2Fwork'), true);
});

test('buildPublicAccessConfig enables public mode with local origins', async () => {
  const cfg = await buildPublicAccessConfig({
    public: true,
    origins: ['https://example.test:443'],
  }, '8001');

  assert.equal(cfg.enabled, true);
  assert.equal(cfg.ok, true);
  assert.equal(cfg.env.PUBLIC_EXPOSURE_MODE, 'true');
  assert.equal(cfg.origins.includes('https://example.test'), true);
  assert.equal(cfg.origins.includes('http://localhost:8001'), true);
  assert.equal(cfg.origins.includes('http://127.0.0.1:8001'), true);
});
