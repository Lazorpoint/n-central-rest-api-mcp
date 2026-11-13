/**
 * Tool-registration helpers. Pure functions only — no I/O, no globals.
 */

import { sanitizeLogString } from './logging.js';

export const COMPOSITION_LIMITS = Object.freeze({
  calls: 10,
  concurrency: 5,
  pages: 20,
  records: 10_000,
  resultBytes: 256 * 1024,
});

const EXPLICIT_SENSITIVE_TOOLS = new Set([
  'get_registration_token',
  'list_users',
  'list_user_roles',
  'get_user_role',
  'list_access_groups',
  'get_access_group',
  'get_psa_ticket',
]);

export function isSensitiveTool(tool) {
  return tool.writeScope !== 'read' || EXPLICIT_SENSITIVE_TOOLS.has(tool.name);
}

/** @param {unknown} message */
export function sanitizeToolError(message) {
  const text = sanitizeLogString(String(message ?? 'Unknown upstream error'));
  return text.length > 300 ? `${text.slice(0, 300)}...` : text;
}

/**
 * Build the internal form of the standard curated result.
 * @param {unknown} data
 * @param {string[]} operations
 * @param {{component: string, code?: string, message?: string}[]} [errors]
 * @param {{page?: unknown, truncated?: boolean}} [options]
 */
export function createCapabilityResult(data, operations, errors = [], options = {}) {
  const safeErrors = errors.map((error) => ({
    component: error.component,
    code: error.code || 'UPSTREAM_ERROR',
    message: sanitizeToolError(error.message),
  }));
  return {
    data,
    meta: {
      operations: [...new Set(operations)],
      partial: safeErrors.length > 0,
      errors: safeErrors,
      page: options.page ?? null,
      truncated: options.truncated === true,
    },
  };
}

function serializedBytes(value) {
  return Buffer.byteLength(JSON.stringify(value), 'utf8');
}

function fitResultToLimit(result) {
  if (serializedBytes(result) <= COMPOSITION_LIMITS.resultBytes) return result;
  const fitted = structuredClone(result);
  fitted.meta.truncated = true;
  if (Array.isArray(fitted.data)) {
    let low = 0;
    let high = fitted.data.length;
    while (low < high) {
      const mid = Math.ceil((low + high) / 2);
      const candidate = { ...fitted, data: fitted.data.slice(0, mid) };
      if (serializedBytes(candidate) <= COMPOSITION_LIMITS.resultBytes) low = mid;
      else high = mid - 1;
    }
    fitted.data = fitted.data.slice(0, low);
  } else {
    fitted.data = { omitted: true, reason: 'Result exceeded the 256 KiB structured-result limit.' };
  }
  if (serializedBytes(fitted) > COMPOSITION_LIMITS.resultBytes) {
    fitted.meta.errors = [];
  }
  return fitted;
}

/** @param {{data: unknown, meta: Record<string, any>}} result */
export function summarizeMcpResult(result) {
  const { data, meta } = result;
  let summary;
  if (Array.isArray(data)) {
    summary = `Returned ${data.length} record${data.length === 1 ? '' : 's'}.`;
  } else if (typeof data === 'string') {
    summary = `Returned a text result (${Buffer.byteLength(data, 'utf8')} bytes).`;
  } else if (data && typeof data === 'object') {
    const objectData = /** @type {Record<string, any>} */ (data);
    const nested = Array.isArray(objectData.data) ? objectData.data : null;
    if (nested) {
      summary = `Returned ${nested.length} record${nested.length === 1 ? '' : 's'} in a paged result.`;
    } else {
      const keys = Object.keys(objectData);
      summary = keys.length
        ? `Returned an object with fields: ${keys.slice(0, 12).join(', ')}${keys.length > 12 ? ', …' : ''}.`
        : 'Returned an empty object.';
    }
  } else if (data == null) {
    summary = 'Returned no data.';
  } else {
    summary = `Returned a ${typeof data} value.`;
  }
  if (meta.page?.pageNumber != null) {
    summary += ` Page ${meta.page.pageNumber}${meta.page.totalPages != null ? ` of ${meta.page.totalPages}` : ''}.`;
    if (meta.page.hasNextPage) summary += ` Request pageNumber=${Number(meta.page.pageNumber) + 1} for more.`;
  }
  if (meta.partial) summary += ` ${meta.errors?.length || 0} component error(s) occurred.`;
  if (meta.truncated) summary += ' The structured result was truncated.';
  return summary;
}

/** @param {Record<string, any>} tool @param {{data: unknown, meta: Record<string, any>}} result */
export function toMcpResult(tool, result) {
  const structuredContent = fitResultToLimit(createCapabilityResult(
    result?.data,
    result?.meta?.operations || tool.operations || [],
    result?.meta?.errors || [],
    { page: result?.meta?.page, truncated: result?.meta?.truncated },
  ));
  return {
    content: [{ type: /** @type {const} */ ('text'), text: renderMcpResultText(structuredContent) }],
    structuredContent,
  };
}

/**
 * The summary followed by the data itself. Upstream sends the summary alone,
 * which assumes the client shows the model structuredContent. claude.ai shows
 * it only the text, so a summary-only result tells the model how many devices
 * matched but not which ones. Compact JSON keeps the text no larger than the
 * structured result that fitResultToLimit has already capped.
 * @param {{data: unknown, meta: Record<string, any>}} result
 */
export function renderMcpResultText(result) {
  const parts = [summarizeMcpResult(result)];
  const { data, meta } = result;
  if (typeof data === 'string') parts.push(data);
  else if (data != null) parts.push(JSON.stringify(data));
  if (meta.errors?.length) parts.push(`Errors: ${JSON.stringify(meta.errors)}`);
  return parts.join('\n\n');
}

/** @param {Record<string, any>} tool @param {Record<string, unknown>} args */
export async function executeCuratedTool(tool, args) {
  try {
    return toMcpResult(tool, await tool.handler(args));
  } catch (error) {
    return {
      content: [{ type: 'text', text: `Error: ${sanitizeToolError(error?.message)}` }],
      isError: true,
    };
  }
}

/**
 * Returns true if a tool's writeScope is permitted by the current write mode.
 *   scope 'read'        → always allowed
 *   scope 'write'       → allowed in 'write' and 'full' modes
 *   scope 'destructive' → allowed only in 'full' mode
 *   unknown scope       → denied
 */
export function isToolAllowed(tool, writeMode) {
  const scope = tool.writeScope || 'read';
  if (scope === 'read') return true;
  if (scope === 'write') return writeMode === 'write' || writeMode === 'full';
  if (scope === 'destructive') return writeMode === 'full';
  return false;
}

/**
 * Build MCP tool annotations from our internal writeScope tag.
 * Maps to the spec's readOnlyHint / destructiveHint / openWorldHint.
 */
export function buildToolAnnotations(tool) {
  const scope = tool.writeScope || 'read';
  return {
    readOnlyHint: scope === 'read',
    destructiveHint: scope === 'destructive',
    openWorldHint: true, // calls a remote API
  };
}
