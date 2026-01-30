import 'dotenv/config';
import { createServer } from 'node:http';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';
const API_BASE_URL = process.env.API_BASE_URL ?? 'http://localhost:3000';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? API_BASE_URL;
const API_KEY = process.env.API_KEY ?? '';
const PORT = Number(process.env.MCP_PORT ?? 4000);
const ALLOWED_ORIGINS = (process.env.MCP_ALLOWED_ORIGINS ?? '')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);
const server = new McpServer({
    name: 'A2ABench',
    version: '0.1.0'
});
async function apiGet(path, params) {
    const url = new URL(path, API_BASE_URL);
    if (params) {
        Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));
    }
    const headers = { accept: 'application/json' };
    if (API_KEY) {
        headers.authorization = `Bearer ${API_KEY}`;
    }
    const response = await fetch(url, { headers });
    return response;
}
server.registerTool('search', {
    title: 'Search questions',
    description: 'Search questions by keyword and return canonical URLs.',
    inputSchema: {
        query: z.string().min(1)
    }
}, async ({ query }) => {
    const response = await apiGet('/api/v1/search', { q: query });
    if (!response.ok) {
        return {
            content: [
                {
                    type: 'text',
                    text: JSON.stringify({ results: [] })
                }
            ]
        };
    }
    const data = (await response.json());
    const results = (data.results ?? []).map((item) => ({
        id: item.id,
        title: item.title,
        url: `${PUBLIC_BASE_URL}/q/${item.id}`
    }));
    return {
        content: [
            {
                type: 'text',
                text: JSON.stringify({ results })
            }
        ]
    };
});
server.registerTool('fetch', {
    title: 'Fetch question thread',
    description: 'Fetch a question and its answers by id.',
    inputSchema: {
        id: z.string().min(1)
    }
}, async ({ id }) => {
    const response = await apiGet(`/api/v1/questions/${id}`);
    if (!response.ok) {
        return {
            content: [
                {
                    type: 'text',
                    text: JSON.stringify({ id, error: 'Not found' })
                }
            ]
        };
    }
    const data = await response.json();
    return {
        content: [
            {
                type: 'text',
                text: JSON.stringify(data)
            }
        ]
    };
});
function isOriginAllowed(origin) {
    if (!origin)
        return true;
    if (ALLOWED_ORIGINS.length === 0)
        return false;
    return ALLOWED_ORIGINS.includes(origin);
}
function applyCors(res, origin) {
    if (!origin)
        return;
    if (!isOriginAllowed(origin))
        return;
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, mcp-session-id, mcp-protocol-version, last-event-id');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
}
async function main() {
    const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: undefined
    });
    await server.connect(transport);
    const httpServer = createServer(async (req, res) => {
        if (!req.url || !req.method) {
            res.statusCode = 400;
            res.end('Bad request');
            return;
        }
        const origin = req.headers.origin;
        applyCors(res, origin);
        if (req.method === 'OPTIONS') {
            res.statusCode = 204;
            res.end();
            return;
        }
        if (!isOriginAllowed(origin)) {
            res.statusCode = 403;
            res.end('Origin not allowed');
            return;
        }
        if (!req.url.startsWith('/mcp')) {
            res.statusCode = 404;
            res.end('Not found');
            return;
        }
        if (req.method === 'GET') {
            await transport.handleRequest(req, res);
            return;
        }
        if (req.method !== 'POST') {
            res.statusCode = 405;
            res.end('Method not allowed');
            return;
        }
        let body = '';
        req.on('data', (chunk) => {
            body += chunk;
        });
        req.on('end', async () => {
            try {
                const json = body.length ? JSON.parse(body) : undefined;
                await transport.handleRequest(req, res, json);
            }
            catch (err) {
                res.statusCode = 400;
                res.end('Invalid JSON');
            }
        });
    });
    httpServer.listen(PORT, '0.0.0.0', () => {
        console.log(`MCP remote server listening on :${PORT}`);
    });
}
main().catch((err) => {
    console.error(err);
    process.exit(1);
});
