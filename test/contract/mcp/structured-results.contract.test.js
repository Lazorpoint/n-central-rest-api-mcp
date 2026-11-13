import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { coreTools } from '../../../src/tools/core.js';
import { createCapabilityResult, toMcpResult } from '../../../src/tool-registry.js';
import { assertRedacted, assertSerializedSize, assertStructuredResult } from './result-assertions.js';

describe('curated MCP structured results', () => {
  it('returns structuredContent plus text that leads with a summary and carries the data', () => {
    const result = toMcpResult(coreTools[0], createCapabilityResult({ healthy: true }, ['GET /api/health']));
    assertStructuredResult(result);
    assert.deepEqual(result.structuredContent.data, { healthy: true });
    assert.equal(result.content[0].text, 'Returned an object with fields: healthy.\n\n{"healthy":true}');
  });

  it('puts device ids in the text, because claude.ai shows the model only the text', () => {
    const result = toMcpResult(coreTools[5], createCapabilityResult([
      { deviceId: 1189203344, longName: 'SRV-DC01' },
      { deviceId: 1189203345, longName: 'SRV-SQL01' },
    ], ['GET /api/devices']));
    const text = result.content[0].text;
    assert.ok(text.startsWith('Returned 2 records.\n\n'));
    assert.deepEqual(JSON.parse(text.split('\n\n')[1]), result.structuredContent.data);
    assert.match(text, /1189203344/);
    assert.match(text, /1189203345/);
  });

  it('leads with a summary of arrays, paged data, text, partial results, and truncation', () => {
    const firstLine = (/** @type {any} */ r) => r.content[0].text.split('\n\n')[0];
    const array = toMcpResult(coreTools[5], createCapabilityResult([{ id: 1 }, { id: 2 }], ['GET /api/devices']));
    assert.equal(firstLine(array), 'Returned 2 records.');

    const paged = toMcpResult(coreTools[5], createCapabilityResult({ data: [{ id: 1 }] }, ['GET /api/devices']));
    assert.equal(firstLine(paged), 'Returned 1 record in a paged result.');

    const nextPage = toMcpResult(coreTools[7], createCapabilityResult(
      [{ id: 1 }], ['GET /api/issues'], [],
      { page: { pageNumber: 2, totalPages: 4, hasNextPage: true } },
    ));
    assert.equal(firstLine(nextPage), 'Returned 1 record. Page 2 of 4. Request pageNumber=3 for more.');

    const text = toMcpResult(coreTools[10], createCapabilityResult('name,id\nSRV-SQL01,1', ['GET /api/device-filters']));
    assert.match(text.content[0].text, /^Returned a text result \(\d+ bytes\)\.\n\nname,id\nSRV-SQL01,1$/);

    const partial = toMcpResult(coreTools[0], createCapabilityResult({}, ['GET /api/health'], [
      { component: 'extra', message: 'failed' },
    ], { truncated: true }));
    assert.match(firstLine(partial), /1 component error\(s\).*truncated/);
    assert.match(partial.content[0].text, /Errors: .*"component":"extra"/);

    const empty = toMcpResult(coreTools[0], createCapabilityResult(null, ['GET /api/health']));
    assert.equal(empty.content[0].text, 'Returned no data.');
  });

  it('truncates oversized arrays to 256 KiB and marks metadata', () => {
    const rows = Array.from({ length: 4000 }, (_, id) => ({ id, value: 'x'.repeat(200) }));
    const result = toMcpResult(coreTools[5], createCapabilityResult(rows, ['GET /api/devices']));
    assertStructuredResult(result);
    assert.equal(result.structuredContent.meta.truncated, true);
    assertSerializedSize(result.structuredContent, 256 * 1024);
  });

  it('redacts component error messages and token-like material', () => {
    const internal = createCapabilityResult(null, ['GET /api/example'], [{
      component: 'optional', code: 'UPSTREAM_ERROR', message: 'Bearer secret-token at https://tenant.example.test/api',
    }]);
    const result = toMcpResult(coreTools[0], internal);
    assertRedacted(result, ['secret-token', 'tenant.example.test']);
    assert.equal(result.structuredContent.meta.partial, true);
  });
});
