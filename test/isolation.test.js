/**
 * End-to-end and structural isolation tests:
 *  - resource cache is tenant-scoped,
 *  - AsyncLocalStorage context survives async hops and throws when absent in
 *    multi-tenant mode,
 *  - two concurrent sessions through the REAL McpServer + StreamableHTTP
 *    transport never cross tenants (validates ALS propagation through the SDK),
 *  - the boot matrix (multi-tenant vs single-tenant) behaves correctly.
 */
import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

import { als, getContext, makeContext } from '../src/context.js';
import { apiGet } from '../src/client.js';
import { evictTenant } from '../src/auth.js';
import { cached, evictTenantCache } from '../src/resources.js';
import { resolveToolConfiguration } from '../src/toolsets.js';
import { installMockFetch } from './mock-fetch.js';

const ctxA = makeContext('https://a.example.com', 'jwtA');
const ctxB = makeContext('https://b.example.com', 'jwtB');

// ---------------------------------------------------------------------------
// Resource cache isolation
// ---------------------------------------------------------------------------
describe('resource cache isolation', () => {
  after(() => { evictTenantCache(ctxA.key); evictTenantCache(ctxB.key); });

  it('keys the cache by tenant — same URI never leaks across tenants', async () => {
    let n = 0;
    const loadA = async () => ({ who: 'A', n: ++n });
    const loadB = async () => ({ who: 'B', n: ++n });
    const uri = 'ncentral://org-tree';

    const a1 = await als.run(ctxA, () => cached(uri, loadA));
    const b1 = await als.run(ctxB, () => cached(uri, loadB)); // same URI, other tenant
    const a2 = await als.run(ctxA, () => cached(uri, loadA)); // must hit A's cache

    assert.equal(a1.who, 'A');
    assert.equal(b1.who, 'B', 'tenant B must NOT receive tenant A cached value');
    assert.deepEqual(a2, a1, "tenant A's second read should be a cache hit");
  });
});

// ---------------------------------------------------------------------------
// AsyncLocalStorage propagation / fail-loud behavior
// ---------------------------------------------------------------------------
describe('async-context propagation', () => {
  it('survives setTimeout / microtask continuations', async () => {
    const seen = await als.run(ctxA, () => new Promise((resolve) => {
      setTimeout(() => resolve(getContext().key), 5);
    }));
    assert.equal(seen, ctxA.key);
  });

  it('getContext() throws in multi-tenant mode when no store is present', async () => {
    // Fresh module instance with NC_MULTI_TENANT set (query string busts the
    // ESM module cache). Its `als` is separate, which is exactly what we want.
    process.env.NC_MULTI_TENANT = '1';
    const mod = await import('../src/context.js?multitenant=1');
    delete process.env.NC_MULTI_TENANT;

    assert.equal(mod.MULTI_TENANT, true);
    assert.throws(() => mod.getContext(), /No tenant context/);

    const ctx = mod.makeContext('https://a.example.com', 'jwtA');
    assert.equal(mod.als.run(ctx, () => mod.getContext().key), ctx.key);
  });
});

// ---------------------------------------------------------------------------
// End-to-end: two concurrent sessions through the real SDK transport
// ---------------------------------------------------------------------------
function buildMcp() {
  const srv = new McpServer({ name: 'isolation-test', version: '0.0.0' });
  // The tool calls the N-central client, which resolves its tenant from the
  // active async context — i.e. it only works if ALS propagated from the
  // als.run() wrapper, through transport.handleRequest, into this handler.
  srv.tool('whoami', 'Return the tenant host this call reached', {}, async () => {
    const r = await apiGet('/api/whoami');
    return { content: [{ type: 'text', text: JSON.stringify(r) }] };
  });
  return srv;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => { data += c; });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : undefined); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

function startTestServer() {
  const transports = new Map();
  const ctxs = new Map();

  const server = http.createServer(async (req, res) => {
    if (req.method !== 'POST') { res.writeHead(405).end(); return; }
    try {
      const body = await readJson(req);
      const sid = req.headers['mcp-session-id'];

      if (sid && transports.has(sid)) {
        await als.run(ctxs.get(sid), () => transports.get(sid).handleRequest(req, res, body));
        return;
      }
      if (!sid && isInitializeRequest(body)) {
        const fqdn = makeContext(req.headers['x-nc-fqdn'], req.headers['x-nc-jwt']);
        const transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          enableJsonResponse: true, // plain-JSON responses keep the test parser simple
          onsessioninitialized: (id) => { transports.set(id, transport); ctxs.set(id, fqdn); },
        });
        const mcp = buildMcp();
        await mcp.connect(transport);
        await als.run(fqdn, () => transport.handleRequest(req, res, body));
        return;
      }
      res.writeHead(400).end();
    } catch (err) {
      if (!res.headersSent) res.writeHead(500).end();
      else res.end();
      void err;
    }
  });

  return server;
}

async function mcpPost(port, extraHeaders, body) {
  const res = await fetch(`http://127.0.0.1:${port}/mcp`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
      ...extraHeaders,
    },
    body: JSON.stringify(body),
  });
  const sessionId = res.headers.get('mcp-session-id');
  const text = await res.text();
  let json = null;
  if (text) {
    const payload = text
      .split(/\r?\n/)
      .find((line) => line.startsWith('data:'))
      ?.slice('data:'.length)
      .trim() || text;
    try { json = JSON.parse(payload); } catch { json = text; }
  }
  return { status: res.status, sessionId, json };
}

const initMsg = (id) => ({
  jsonrpc: '2.0', id, method: 'initialize',
  params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '0' } },
});
const initializedMsg = () => ({ jsonrpc: '2.0', method: 'notifications/initialized' });
const toolCallMsg = (id) => ({ jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'whoami', arguments: {} } });

function toolHost(callResult) {
  const text = callResult?.json?.result?.content?.[0]?.text;
  return JSON.parse(text).host;
}

describe('end-to-end session isolation through the real MCP transport', () => {
  it('routes two concurrent sessions to their own tenants', async () => {
    const server = startTestServer();
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    const port = server.address().port;
    const mock = installMockFetch();

    try {
      const initA = await mcpPost(port, { 'X-NC-FQDN': 'https://a.example.com', 'X-NC-JWT': 'jwtA' }, initMsg(1));
      const initB = await mcpPost(port, { 'X-NC-FQDN': 'https://b.example.com', 'X-NC-JWT': 'jwtB' }, initMsg(1));
      assert.ok(initA.sessionId, `session A init failed: ${JSON.stringify(initA)}`);
      assert.ok(initB.sessionId, `session B init failed: ${JSON.stringify(initB)}`);

      await mcpPost(port, { 'mcp-session-id': initA.sessionId }, initializedMsg());
      await mcpPost(port, { 'mcp-session-id': initB.sessionId }, initializedMsg());

      // Fire the two tool calls concurrently — the ALS store must keep them apart.
      const [ra, rb] = await Promise.all([
        mcpPost(port, { 'mcp-session-id': initA.sessionId }, toolCallMsg(2)),
        mcpPost(port, { 'mcp-session-id': initB.sessionId }, toolCallMsg(2)),
      ]);

      assert.equal(toolHost(ra), 'a.example.com');
      assert.equal(toolHost(rb), 'b.example.com');
    } finally {
      mock.restore();
      evictTenant(ctxA.key);
      evictTenant(ctxB.key);
      await new Promise((r) => server.close(r));
    }
  });
});

// ---------------------------------------------------------------------------
// Boot matrix
// ---------------------------------------------------------------------------
const indexPath = fileURLToPath(new URL('../index.js', import.meta.url));
const mockLoaderPath = fileURLToPath(new URL('./contract/mock-fetch-loader.js', import.meta.url));

function freePort() {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); });
  });
}

function waitForHealth(port) {
  return new Promise((resolve, reject) => {
    let tries = 0;
    const tick = async () => {
      tries += 1;
      try {
        const r = await fetch(`http://127.0.0.1:${port}/healthz`);
        if (r.ok) { resolve(); return; }
      } catch { /* not up yet */ }
      if (tries > 60) { reject(new Error('server did not become healthy')); return; }
      setTimeout(tick, 100);
    };
    tick();
  });
}

function cleanEnv(extra) {
  const base = { ...process.env };
  for (const k of ['NC_SERVER_URL', 'NC_JWT_TOKEN', 'NC_MULTI_TENANT', 'MCP_PORT', 'MCP_API_KEY', 'NC_FQDN_ALLOWLIST', 'MCP_MAX_BODY_SIZE', 'MCP_RATE_LIMIT_WINDOW_MS', 'MCP_RATE_LIMIT_MAX', 'MCP_MAX_SESSIONS', 'MCP_SESSION_TTL_MS', 'NC_MAX_TENANTS', 'NC_MAX_RETRIES', 'NC_RETRY_DELAY_MS', 'NC_REQUEST_TIMEOUT_MS', 'NC_RESOURCE_CACHE_TTL_MS']) {
    delete base[k];
  }
  return { ...base, ...extra };
}

function runBoot(env, { readyRe, waitMs = 2500 } = {}) {
  const base = { ...process.env };
  for (const k of ['NC_SERVER_URL', 'NC_JWT_TOKEN', 'NC_MULTI_TENANT', 'MCP_PORT', 'MCP_API_KEY', 'NC_FQDN_ALLOWLIST', 'MCP_MAX_BODY_SIZE', 'MCP_RATE_LIMIT_WINDOW_MS', 'MCP_RATE_LIMIT_MAX', 'MCP_MAX_SESSIONS', 'MCP_SESSION_TTL_MS', 'NC_MAX_TENANTS', 'NC_MAX_RETRIES', 'NC_RETRY_DELAY_MS', 'NC_REQUEST_TIMEOUT_MS', 'NC_RESOURCE_CACHE_TTL_MS']) {
    delete base[k];
  }
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [indexPath], {
      env: { ...base, ...env },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stderr = '';
    let settled = false;
    const done = (exitCode) => {
      if (settled) return;
      settled = true;
      try { child.kill('SIGKILL'); } catch { /* ignore */ }
      resolve({ exitCode, stderr });
    };
    child.stdout.on('data', () => {});
    child.stderr.on('data', (d) => {
      stderr += d;
      if (readyRe && readyRe.test(stderr)) done(null); // booted successfully → still running
    });
    child.on('exit', (code) => done(code));
    setTimeout(() => done(null), waitMs);
  });
}

describe('boot matrix', () => {
  it('rejects multi-tenant + stdio (no MCP_PORT)', async () => {
    const { exitCode, stderr } = await runBoot({ NC_MULTI_TENANT: '1' });
    assert.equal(exitCode, 1);
    assert.match(stderr, /requires HTTP mode/);
  });

  it('rejects single-tenant with no credentials', async () => {
    const { exitCode, stderr } = await runBoot({});
    assert.equal(exitCode, 1);
    assert.match(stderr, /NC_SERVER_URL and NC_JWT_TOKEN .* are required/s);
  });

  it('rejects an insecure or non-origin single-tenant server URL at startup', async () => {
    for (const url of ['http://host.example.com', 'https://host.example.com/api', 'https://user:pass@host.example.com']) {
      const { exitCode, stderr } = await runBoot({ NC_SERVER_URL: url, NC_JWT_TOKEN: 'a.b.c' });
      assert.equal(exitCode, 1, url);
      assert.match(stderr, /https|path|credentials/, url);
    }
  });

  it('boots multi-tenant over HTTP without env credentials', async () => {
    const port = await freePort();
    const { exitCode, stderr } = await runBoot(
      { NC_MULTI_TENANT: '1', MCP_PORT: String(port), MCP_API_KEY: 'test-key' },
      { readyRe: /Multi-tenant mode|REST API MCP Server on http/ },
    );
    assert.equal(exitCode, null, `expected the server to keep running, exited with ${exitCode}\n${stderr}`);
    assert.match(stderr, /Multi-tenant mode/);
  });

  it('rejects every explicit invalid numeric setting with its setting name', async () => {
    for (const [name, value] of [
      ['MCP_PORT', '70000'],
      ['MCP_MAX_SESSIONS', '1.5'],
      ['NC_MAX_RETRIES', '-1'],
      ['NC_REQUEST_TIMEOUT_MS', 'not-a-number'],
      ['NC_RESOURCE_CACHE_TTL_MS', '-1'],
    ]) {
      const { exitCode, stderr } = await runBoot({
        NC_MULTI_TENANT: '1',
        MCP_PORT: '3100',
        MCP_API_KEY: 'test-key',
        [name]: value,
      });
      assert.equal(exitCode, 1, name);
      assert.match(stderr, new RegExp(`Invalid ${name}`), name);
    }
  });
});

// ---------------------------------------------------------------------------
// SSRF / header-override guard (single-tenant must ignore client X-NC-* headers)
// ---------------------------------------------------------------------------
describe('single-tenant SSRF guard', () => {
  it('ignores client X-NC-* headers in single-tenant mode (no override / SSRF)', async () => {
    const port = await freePort();
    const child = spawn(process.execPath, [indexPath], {
      env: cleanEnv({
        NC_SERVER_URL: 'https://env.ncentral.com',
        NC_JWT_TOKEN: 'a.b.c',
        MCP_PORT: String(port),
        MCP_API_KEY: 'k',
        MCP_QUIET: '1',
      }),
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    try {
      await waitForHealth(port);
      // Bogus X-NC headers that WOULD be rejected (400) if honored: http:// + non-JWT.
      // In single-tenant mode they must be ignored and env creds used → init succeeds.
      const r = await mcpPost(port, {
        Authorization: 'Bearer k',
        'X-NC-FQDN': 'http://evil.example.org',
        'X-NC-JWT': 'not-a-jwt',
      }, initMsg(1));
      assert.equal(r.status, 200, `expected env-credential init to succeed (headers ignored), got ${r.status}`);
      assert.ok(r.sessionId, 'expected a session bound to the env tenant');
    } finally {
      child.kill('SIGKILL');
    }
  });
});

describe('real curated registry parity', () => {
  it('advertises and calls structured core tools over stdio', async () => {
    const client = new Client({ name: 'stdio-contract', version: '1' });
    const transport = new StdioClientTransport({
      command: process.execPath,
      args: ['--import', mockLoaderPath, indexPath],
      env: cleanEnv({ NC_SERVER_URL: 'https://stdio.example.test', NC_JWT_TOKEN: 'a.b.c', MCP_QUIET: '1' }),
      stderr: 'pipe',
    });
    try {
      await client.connect(transport);
      const tools = await client.listTools();
      assert.deepEqual(tools.tools.map((tool) => tool.name), [
        'get_server_status', 'validate_session', 'get_current_user', 'search_organizations',
        'get_organization_context', 'search_devices', 'get_device_context', 'list_active_issues',
        'list_device_scheduled_tasks', 'get_scheduled_task_context', 'run_report', 'list_job_statuses',
      ]);
      const deviceContext = tools.tools.find(({ name }) => name === 'get_device_context');
      const pageSizeSchema = deviceContext.inputSchema.properties.noteOptions.properties.pageSize;
      assert.ok(pageSizeSchema.anyOf.some((alternative) => alternative.const === -1));
      const invalid = await client.callTool({
        name: 'get_device_context',
        arguments: { deviceId: '1', include: ['notes'], noteOptions: { pageSize: 0 } },
      });
      assert.equal(invalid.isError, true);
      const unexpected = await client.callTool({
        name: 'validate_session', arguments: { unexpected: true },
      });
      assert.equal(unexpected.isError, true);
      const unfilteredOrganizations = await client.callTool({
        name: 'search_organizations',
        arguments: { organizationType: 'customer', pageNumber: 1, pageSize: 2 },
      });
      assert.equal(unfilteredOrganizations.isError, undefined);
      const unfilteredDevices = await client.callTool({
        name: 'search_devices', arguments: { pageNumber: 1, pageSize: 2 },
      });
      assert.equal(unfilteredDevices.isError, undefined);
      const result = await client.callTool({ name: 'validate_session', arguments: {} });
      assert.ok(result.structuredContent);
      assert.equal(result.isError, undefined);
    } finally { await client.close(); }
  });

  it('advertises and calls the same selected read catalog over HTTP', async () => {
    const port = await freePort();
    const child = spawn(process.execPath, ['--import', mockLoaderPath, indexPath], {
      env: cleanEnv({ NC_SERVER_URL: 'https://http.example.test', NC_JWT_TOKEN: 'a.b.c', MCP_PORT: String(port), MCP_API_KEY: 'k', MCP_QUIET: '1' }),
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    try {
      await waitForHealth(port);
      const headers = { Authorization: 'Bearer k' };
      const init = await mcpPost(port, headers, initMsg(51));
      assert.ok(init.sessionId);
      const sessionHeaders = { ...headers, 'mcp-session-id': init.sessionId };
      await mcpPost(port, sessionHeaders, initializedMsg());
      const listed = await mcpPost(port, sessionHeaders, { jsonrpc: '2.0', id: 52, method: 'tools/list', params: {} });
      assert.ok(listed.json?.result, JSON.stringify(listed));
      assert.equal(listed.json.result.tools.length, 12);
      const called = await mcpPost(port, sessionHeaders, { jsonrpc: '2.0', id: 53, method: 'tools/call', params: { name: 'validate_session', arguments: {} } });
      assert.ok(called.json.result.structuredContent);
    } finally { child.kill('SIGKILL'); }
  });

  it('preserves structured results, session deletion, and graceful HTTP shutdown', async () => {
    const port = await freePort();
    const child = spawn(process.execPath, ['--import', mockLoaderPath, indexPath], {
      env: cleanEnv({ NC_SERVER_URL: 'https://lifecycle.example.test', NC_JWT_TOKEN: 'a.b.c', MCP_PORT: String(port), MCP_API_KEY: 'k', MCP_QUIET: '1' }),
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    try {
      await waitForHealth(port);
      const headers = { Authorization: 'Bearer k' };
      const init = await mcpPost(port, headers, initMsg(71));
      const sessionHeaders = { ...headers, 'mcp-session-id': init.sessionId };
      await mcpPost(port, sessionHeaders, initializedMsg());
      const called = await mcpPost(port, sessionHeaders, { jsonrpc: '2.0', id: 72, method: 'tools/call', params: { name: 'validate_session', arguments: {} } });
      assert.deepEqual(called.json.result.structuredContent.meta.operations, ['GET /api/auth/validate']);
      assert.equal(called.json.result.structuredContent.meta.partial, false);

      const deleted = await fetch(`http://127.0.0.1:${port}/mcp`, {
        method: 'DELETE', headers: { ...sessionHeaders, Accept: 'application/json, text/event-stream' },
      });
      assert.equal(deleted.status, 200);
      const afterDelete = await mcpPost(port, sessionHeaders, { jsonrpc: '2.0', id: 73, method: 'tools/list', params: {} });
      assert.equal(afterDelete.status, 404); // Lazorpoint patch: unknown session is 404 per spec
      const health = await (await fetch(`http://127.0.0.1:${port}/healthz`)).json();
      assert.equal(health.sessions, 0);

      const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
      child.kill('SIGTERM');
      const outcome = await Promise.race([
        exited,
        new Promise((_, reject) => setTimeout(() => reject(new Error('HTTP server did not shut down')), 2000)),
      ]);
      assert.deepEqual(outcome, { code: 0, signal: null });
    } finally {
      if (child.exitCode == null && child.signalCode == null) child.kill('SIGKILL');
    }
  });

  it('keeps stdio and HTTP discovery identical across toolsets and write modes', async () => {
    const configurations = [
      { toolsets: 'core', writeMode: 'read-only' },
      { toolsets: 'operations', writeMode: 'write' },
      { toolsets: 'core,operations,administration,psa,reporting,compatibility', writeMode: 'full' },
    ];

    for (const configuration of configurations) {
      const expected = resolveToolConfiguration({
        rawToolsets: configuration.toolsets,
        rawWriteMode: configuration.writeMode,
      }).tools.map((tool) => tool.name);
      const baseEnv = {
        NC_SERVER_URL: 'https://parity.example.test',
        NC_JWT_TOKEN: 'a.b.c',
        NC_TOOLSETS: configuration.toolsets,
        NC_WRITE_MODE: configuration.writeMode,
        MCP_QUIET: '1',
      };

      const client = new Client({ name: 'stdio-parity', version: '1' });
      const transport = new StdioClientTransport({
        command: process.execPath,
        args: ['--import', mockLoaderPath, indexPath],
        env: cleanEnv(baseEnv),
        stderr: 'pipe',
      });
      let stdioNames;
      try {
        await client.connect(transport);
        stdioNames = (await client.listTools()).tools.map((tool) => tool.name);
      } finally { await client.close(); }

      const port = await freePort();
      const child = spawn(process.execPath, ['--import', mockLoaderPath, indexPath], {
        env: cleanEnv({ ...baseEnv, MCP_PORT: String(port), MCP_API_KEY: 'k' }),
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      let httpNames;
      try {
        await waitForHealth(port);
        const headers = { Authorization: 'Bearer k' };
        const init = await mcpPost(port, headers, initMsg(61));
        const sessionHeaders = { ...headers, 'mcp-session-id': init.sessionId };
        await mcpPost(port, sessionHeaders, initializedMsg());
        const listed = await mcpPost(port, sessionHeaders, { jsonrpc: '2.0', id: 62, method: 'tools/list', params: {} });
        httpNames = listed.json.result.tools.map((tool) => tool.name);
      } finally { child.kill('SIGKILL'); }

      assert.deepEqual(stdioNames, expected, `${configuration.toolsets}/${configuration.writeMode} stdio drift`);
      assert.deepEqual(httpNames, expected, `${configuration.toolsets}/${configuration.writeMode} HTTP drift`);
    }
  });
});
