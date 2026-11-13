/** Streamable HTTP routing, authentication, rate limiting, and session lifecycle. */

import { randomUUID } from 'node:crypto';
import http from 'node:http';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

import { evictTenant } from './auth.js';
import { als, makeContext } from './context.js';
import { auditErrorMetadata, auditLog } from './logging.js';
import { renderPrometheus, setGauge } from './metrics.js';
import { evictTenantCache } from './resources.js';
import {
  looksLikeJwt,
  parseAuthorizationHeader,
  safeCompare,
  validateNcFqdn,
} from './server-utils.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export class ContextError extends Error {}

export function validSessionId(id) {
  return typeof id === 'string' && UUID_RE.test(id);
}

export function authenticateRequest(req, apiKey) {
  if (!apiKey) return true;
  const token = parseAuthorizationHeader(req.headers.authorization);
  return token ? safeCompare(token, apiKey) : false;
}

export function getClientIp(req, trustProxy) {
  if (trustProxy) {
    const forwarded = req.headers['x-forwarded-for'];
    if (typeof forwarded === 'string') return forwarded.split(',')[0].trim();
  }
  return req.socket?.remoteAddress || 'unknown';
}

export function resolveRequestContext(req, { multiTenant, envContext, allowlist }) {
  if (!multiTenant) {
    if (envContext) return envContext;
    throw new ContextError('No N-central credentials configured');
  }

  const fqdnHeader = req.headers['x-nc-fqdn'];
  const jwtHeader = req.headers['x-nc-jwt'];
  if (typeof fqdnHeader !== 'string' || typeof jwtHeader !== 'string') {
    throw new ContextError('Both X-NC-FQDN and X-NC-JWT must be present and single-valued');
  }
  if (!looksLikeJwt(jwtHeader)) throw new ContextError('X-NC-JWT is not a valid JWT');
  try {
    return makeContext(validateNcFqdn(fqdnHeader, allowlist), jwtHeader);
  } catch (error) {
    throw new ContextError(`Invalid X-NC-FQDN: ${error.message}`);
  }
}

export function parseRequestBody(req, maximumBytes) {
  return new Promise((resolve, reject) => {
    let data = '';
    let size = 0;
    let done = false;
    const finish = (callback, value) => {
      if (done) return;
      done = true;
      callback(value);
    };
    req.on('data', (chunk) => {
      size += Buffer.byteLength(chunk);
      if (size > maximumBytes) {
        req.destroy();
        finish(reject, new Error('Request body too large'));
        return;
      }
      data += chunk;
    });
    req.on('end', () => {
      try {
        finish(resolve, data ? JSON.parse(data) : undefined);
      } catch {
        finish(reject, new Error('Invalid JSON body'));
      }
    });
    req.on('error', (error) => finish(reject, error));
  });
}

export function createRateLimiter({ windowMs, maximum, capacity, now = Date.now }) {
  const clients = new Map();
  return {
    allow(client) {
      const timestamp = now();
      let entry = clients.get(client);
      if (!entry || timestamp - entry.windowStart > windowMs) {
        if (!entry && clients.size >= capacity) {
          let oldestClient;
          let oldestStart = Infinity;
          for (const [candidate, value] of clients) {
            if (value.windowStart < oldestStart) {
              oldestClient = candidate;
              oldestStart = value.windowStart;
            }
          }
          if (oldestClient !== undefined) clients.delete(oldestClient);
        }
        entry = { count: 1, windowStart: timestamp };
        clients.set(client, entry);
        return true;
      }
      entry.count += 1;
      return entry.count <= maximum;
    },
    cleanup() {
      const timestamp = now();
      for (const [client, entry] of clients) {
        if (timestamp - entry.windowStart > windowMs * 2) clients.delete(client);
      }
    },
    size: () => clients.size,
  };
}

function runWithContext(context, callback) {
  return context ? als.run(context, callback) : callback();
}

function sendJson(res, status, value, headers = {}) {
  res.writeHead(status, { 'Content-Type': 'application/json', ...headers });
  res.end(JSON.stringify(value));
}

/** Start the HTTP listener and return a bounded shutdown handle. */
export async function startHttpRuntime(options) {
  const {
    port,
    bindAddress,
    apiKey,
    corsOrigin,
    trustProxy,
    maxBodySize,
    rateLimitWindowMs,
    rateLimitMaximum,
    maxRateLimitEntries,
    maxSessions,
    sessionTtlMs,
    metricsRequireAuth,
    multiTenant,
    envContext,
    fqdnAllowlist,
    createServer,
  } = options;
  const allowedOrigins = (corsOrigin || '').split(',').map((value) => value.trim()).filter(Boolean);
  const rateLimiter = createRateLimiter({
    windowMs: rateLimitWindowMs,
    maximum: rateLimitMaximum,
    capacity: maxRateLimitEntries,
  });
  const transports = new Map();
  const sessionContexts = new Map();
  const tenantReferences = new Map();
  const sessionActivity = new Map();

  function addTenantReference(key, sessionId) {
    let references = tenantReferences.get(key);
    if (!references) {
      references = new Set();
      tenantReferences.set(key, references);
    }
    references.add(sessionId);
  }

  function teardownSession(sessionId) {
    transports.delete(sessionId);
    sessionActivity.delete(sessionId);
    const context = sessionContexts.get(sessionId);
    if (!context) return;
    sessionContexts.delete(sessionId);
    const references = tenantReferences.get(context.key);
    if (!references) return;
    references.delete(sessionId);
    if (references.size === 0) {
      tenantReferences.delete(context.key);
      evictTenant(context.key);
      evictTenantCache(context.key);
    }
  }

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${port}`);
    const clientIp = getClientIp(req, trustProxy);

    const requestOrigin = req.headers.origin;
    if (typeof requestOrigin === 'string' && allowedOrigins.includes(requestOrigin)) {
      res.setHeader('Access-Control-Allow-Origin', requestOrigin);
      res.setHeader('Vary', 'Origin');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, mcp-session-id, X-NC-FQDN, X-NC-JWT');
      res.setHeader('Access-Control-Expose-Headers', 'mcp-session-id');
    }

    if (req.method === 'OPTIONS') {
      res.writeHead(res.hasHeader('Access-Control-Allow-Origin') ? 204 : 405,
        res.hasHeader('Access-Control-Allow-Origin') ? {} : { Allow: 'POST, GET, DELETE' });
      res.end();
      return;
    }
    if (url.pathname === '/healthz' || url.pathname === '/health') {
      sendJson(res, 200, { status: 'ok', sessions: transports.size });
      return;
    }
    if (url.pathname === '/metrics') {
      if (metricsRequireAuth && !authenticateRequest(req, apiKey)) {
        res.writeHead(401, { 'Content-Type': 'text/plain' });
        res.end('Unauthorized');
        return;
      }
      setGauge('nc_mcp_active_sessions', transports.size);
      res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
      res.end(renderPrometheus());
      return;
    }
    if (!rateLimiter.allow(clientIp)) {
      auditLog('rate_limited', { ip: clientIp, path: url.pathname });
      sendJson(res, 429,
        { jsonrpc: '2.0', error: { code: -32000, message: 'Too many requests' }, id: null },
        { 'Retry-After': '60' });
      return;
    }
    if (!authenticateRequest(req, apiKey)) {
      auditLog('auth_failed', { ip: clientIp, path: url.pathname });
      sendJson(res, 401, { jsonrpc: '2.0', error: { code: -32000, message: 'Unauthorized' }, id: null });
      return;
    }
    if (url.pathname !== '/mcp') {
      sendJson(res, 404, { error: 'Not found' });
      return;
    }

    if (req.method === 'POST') {
      try {
        const body = await parseRequestBody(req, maxBodySize);
        const sessionId = req.headers['mcp-session-id'];
        if (sessionId && validSessionId(sessionId) && transports.has(sessionId)) {
          sessionActivity.set(sessionId, Date.now());
          await runWithContext(sessionContexts.get(sessionId), () => (
            transports.get(sessionId).handleRequest(req, res, body)
          ));
        } else if (!sessionId && isInitializeRequest(body)) {
          if (transports.size >= maxSessions) {
            auditLog('session_limit_reached', { ip: clientIp, count: transports.size });
            sendJson(res, 503, {
              jsonrpc: '2.0',
              error: { code: -32000, message: 'Server session limit reached' },
              id: null,
            });
            return;
          }
          let context;
          try {
            context = resolveRequestContext(req, {
              multiTenant,
              envContext,
              allowlist: fqdnAllowlist,
            });
          } catch (error) {
            if (error instanceof ContextError) {
              auditLog('context_rejected', { ip: clientIp, reasonCode: 'INVALID_TENANT_CONTEXT' });
              sendJson(res, 400, {
                jsonrpc: '2.0',
                error: { code: -32000, message: error.message },
                id: null,
              });
              return;
            }
            throw error;
          }
          auditLog('session_init', { ip: clientIp, fqdn: context.fqdn, tenant: context.key });
          const transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: () => randomUUID(),
            onsessioninitialized: (newSessionId) => {
              transports.set(newSessionId, transport);
              sessionContexts.set(newSessionId, context);
              addTenantReference(context.key, newSessionId);
              sessionActivity.set(newSessionId, Date.now());
            },
          });
          transport.onclose = () => {
            if (transport.sessionId) teardownSession(transport.sessionId);
          };
          const server = createServer();
          await server.connect(transport);
          await runWithContext(context, () => transport.handleRequest(req, res, body));
        } else if (sessionId) {
          // Local patch (Lazorpoint, re-derived for v3 2026-10-22): an unknown or
          // expired session must be 404 per the MCP Streamable HTTP spec, so clients
          // re-initialize instead of failing forever. Upstream returns 400 here,
          // which strands claude.ai connectors after every restart.
          sendJson(res, 404, {
            jsonrpc: '2.0',
            error: { code: -32001, message: 'Session not found' },
            id: null,
          });
        } else {
          sendJson(res, 400, {
            jsonrpc: '2.0',
            error: { code: -32000, message: 'Bad request: missing session' },
            id: null,
          });
        }
      } catch (error) {
        auditLog('request_error', { method: 'POST', path: '/mcp', ...auditErrorMetadata(error) });
        if (!res.headersSent) {
          sendJson(res, 500, {
            jsonrpc: '2.0', error: { code: -32603, message: 'Internal error' }, id: null,
          });
        }
      }
      return;
    }

    const sessionId = req.headers['mcp-session-id'];
    if ((req.method === 'GET' || req.method === 'DELETE')
      && (!sessionId || !validSessionId(sessionId) || !transports.has(sessionId))) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Invalid session');
      return;
    }
    if (req.method === 'GET') {
      sessionActivity.set(sessionId, Date.now());
      await runWithContext(sessionContexts.get(sessionId), () => transports.get(sessionId).handleRequest(req, res));
      return;
    }
    if (req.method === 'DELETE') {
      auditLog('session_delete', { sessionId, ip: clientIp });
      await transports.get(sessionId).handleRequest(req, res);
      return;
    }
    res.writeHead(405, { 'Content-Type': 'text/plain' });
    res.end('Method not allowed');
  });

  httpServer.on('clientError', (_error, socket) => {
    if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
  });

  const rateCleanup = setInterval(() => rateLimiter.cleanup(), rateLimitWindowMs);
  rateCleanup.unref();
  const sessionCleanup = setInterval(() => {
    const now = Date.now();
    for (const sessionId of transports.keys()) {
      if (!sessionActivity.has(sessionId)) {
        sessionActivity.set(sessionId, now);
      } else if (now - sessionActivity.get(sessionId) > sessionTtlMs) {
        auditLog('session_expired', { sessionId });
        const transport = transports.get(sessionId);
        teardownSession(sessionId);
        try { transport?.close(); } catch { /* best-effort expiry */ }
      }
    }
  }, Math.min(5 * 60_000, sessionTtlMs));
  sessionCleanup.unref();

  await new Promise((resolve, reject) => {
    const onError = (error) => reject(error);
    httpServer.once('error', onError);
    httpServer.listen(port, bindAddress, () => {
      httpServer.off('error', onError);
      resolve();
    });
  });

  let closing = false;
  return {
    httpServer,
    sessionCount: () => transports.size,
    async shutdown() {
      if (closing) return;
      closing = true;
      clearInterval(rateCleanup);
      clearInterval(sessionCleanup);
      for (const [sessionId, transport] of transports) {
        teardownSession(sessionId);
        try { await transport.close(); } catch { /* best-effort shutdown */ }
      }
      if (httpServer.listening) {
        await new Promise((resolve) => httpServer.close(resolve));
      }
    },
  };
}
