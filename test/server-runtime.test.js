import { readFileSync } from 'node:fs';
import { Readable } from 'node:stream';
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';

import {
  ContextError,
  authenticateRequest,
  createRateLimiter,
  getClientIp,
  parseRequestBody,
  resolveRequestContext,
  validSessionId,
} from '../src/http-runtime.js';
import { createMcpServer } from '../src/mcp-server.js';
import { inc, renderPrometheus, setGauge } from '../src/metrics.js';
import { createCapabilityResult } from '../src/tool-registry.js';
import { CURATED_OUTPUT_SCHEMA } from '../src/tools/core.js';

describe('focused server runtime modules', () => {
  it('keeps the executable entry point limited to configuration and lifecycle wiring', () => {
    const source = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
    assert.ok(source.split('\n').length < 200);
    assert.doesNotMatch(source, /http\.createServer|registerTool|class ContextError/);
  });

  it('constructs the MCP registration boundary independently', () => {
    const server = createMcpServer([]);
    assert.equal(typeof server.connect, 'function');
    assert.equal(typeof server.registerTool, 'function');
  });

  it('enforces strict inputs and renders successful and failed handlers in memory', async () => {
    const inputSchema = {
      type: 'object', additionalProperties: false,
      properties: { value: { type: 'string' } }, required: ['value'],
    };
    const tools = [
      {
        name: 'synthetic_read', description: 'Synthetic successful read used by the runtime test.',
        inputSchema, outputSchema: CURATED_OUTPUT_SCHEMA,
        operations: ['GET /api/synthetic'], toolsets: ['core'], writeScope: 'read',
        handler: async ({ value }) => value === 'large-partial'
          ? createCapabilityResult(
            Array.from({ length: 400 }, (_, index) => ({ index, value: 'x'.repeat(1000) })),
            ['GET /api/synthetic'],
            [{ component: 'optional', code: 'NOT_FOUND', message: 'not found' }],
          )
          : createCapabilityResult({ value }, ['GET /api/synthetic']),
      },
      {
        name: 'synthetic_write', description: 'Synthetic failed write used by the runtime test.',
        inputSchema, outputSchema: CURATED_OUTPUT_SCHEMA,
        operations: ['POST /api/synthetic'], toolsets: ['operations'], writeScope: 'write',
        handler: async () => { throw new Error('Bearer secret at https://tenant.example.test/api'); },
      },
    ];
    const server = createMcpServer(tools);
    const client = new Client({ name: 'runtime-test', version: '1' });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    await client.connect(clientTransport);
    try {
      const invalid = await client.callTool({
        name: 'synthetic_read', arguments: { value: 'ok', unexpected: true },
      });
      assert.equal(invalid.isError, true);

      const successful = await client.callTool({
        name: 'synthetic_read', arguments: { value: 'ok' },
      });
      assert.deepEqual(successful.structuredContent.data, { value: 'ok' });
      assert.equal(successful.content[0].text.includes('"value":"ok"'), true);

      const failed = await client.callTool({
        name: 'synthetic_write', arguments: { value: 'fail' },
      });
      assert.equal(failed.isError, true);
      assert.equal(JSON.stringify(failed).includes('secret'), false);
      assert.equal(JSON.stringify(failed).includes('tenant.example.test'), false);
      const bounded = await client.callTool({
        name: 'synthetic_read', arguments: { value: 'large-partial' },
      });
      assert.equal(bounded.structuredContent.meta.partial, true);
      assert.equal(bounded.structuredContent.meta.truncated, true);
      const metrics = renderPrometheus();
      assert.match(metrics, /nc_mcp_tool_duration_ms_total\{success="true",tool="synthetic_read"\}/);
      assert.match(metrics, /nc_mcp_tool_response_bytes_total\{success="true",tool="synthetic_read"\}/);
      assert.match(metrics, /nc_mcp_upstream_operations_total\{tool="synthetic_read"\} 2/);
      assert.match(metrics, /nc_mcp_tool_response_bytes_total\{success="false",tool="synthetic_write"\}/);
      assert.match(metrics, /nc_mcp_partial_results_total\{tool="synthetic_read"\}/);
      assert.match(metrics, /nc_mcp_truncated_results_total\{tool="synthetic_read"\}/);
    } finally {
      await client.close();
      await server.close();
    }
  });

  it('enforces rate limits, bounded client tracking, and window rollover deterministically', () => {
    let now = 1_000;
    const limiter = createRateLimiter({ windowMs: 100, maximum: 2, capacity: 2, now: () => now });
    assert.equal(limiter.allow('a'), true);
    assert.equal(limiter.allow('a'), true);
    assert.equal(limiter.allow('a'), false);
    assert.equal(limiter.allow('b'), true);
    assert.equal(limiter.allow('c'), true);
    assert.equal(limiter.size(), 2);
    now += 101;
    assert.equal(limiter.allow('a'), true);
    now += 201;
    limiter.cleanup();
    assert.equal(limiter.size(), 0);
  });

  it('parses valid request bodies and rejects malformed or oversized JSON', async () => {
    const valid = Readable.from([Buffer.from('{"ok":true}')]);
    assert.deepEqual(await parseRequestBody(valid, 32), { ok: true });
    await assert.rejects(parseRequestBody(Readable.from(['{']), 32), /Invalid JSON body/);
    await assert.rejects(parseRequestBody(Readable.from(['12345']), 4), /too large/);
  });

  it('keeps single-tenant credentials authoritative and validates multi-tenant headers', () => {
    const envContext = { fqdn: 'https://fixed.example.test', jwt: 'fixed', key: 'fixed-key' };
    const overridden = resolveRequestContext({
      headers: { 'x-nc-fqdn': 'https://evil.example.test', 'x-nc-jwt': 'a.b.c' },
    }, { multiTenant: false, envContext, allowlist: [] });
    assert.equal(overridden, envContext);

    assert.throws(() => resolveRequestContext({ headers: {} }, {
      multiTenant: true,
      envContext: null,
      allowlist: ['allowed.example.test'],
    }), ContextError);

    const context = resolveRequestContext({
      headers: { 'x-nc-fqdn': 'https://api.allowed.example.test', 'x-nc-jwt': 'a.b.c' },
    }, { multiTenant: true, envContext: null, allowlist: ['allowed.example.test'] });
    assert.equal(context.fqdn, 'https://api.allowed.example.test');
  });

  it('validates sessions, bearer authentication, and trusted proxy addresses', () => {
    assert.equal(validSessionId('123e4567-e89b-12d3-a456-426614174000'), true);
    assert.equal(validSessionId('not-a-session'), false);
    const req = {
      headers: { authorization: 'Bearer expected', 'x-forwarded-for': '192.0.2.1, 192.0.2.2' },
      socket: { remoteAddress: '127.0.0.1' },
    };
    assert.equal(authenticateRequest(req, 'expected'), true);
    assert.equal(authenticateRequest(req, 'wrong'), false);
    assert.equal(getClientIp(req, true), '192.0.2.1');
    assert.equal(getClientIp(req, false), '127.0.0.1');
  });

  it('renders deterministic escaped metrics for runtime counters and gauges', () => {
    inc('runtime_test_total', { z: 'quoted"value', a: 1 }, 2);
    setGauge('runtime_test_gauge', 3);
    const output = renderPrometheus();
    assert.match(output, /# TYPE runtime_test_total counter/);
    assert.match(output, /runtime_test_total\{a="1",z="quoted\\"value"\} 2/);
    assert.match(output, /# TYPE runtime_test_gauge gauge\nruntime_test_gauge 3/);
  });
});
