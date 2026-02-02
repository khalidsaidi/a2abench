export default {
  async fetch(request) {
    const incoming = new URL(request.url);
    const target = new URL('https://a2abench-mcp.web.app');
    target.pathname = incoming.pathname === '/healthz' ? '/healthz/' : incoming.pathname;
    target.search = incoming.search;

    const proxiedRequest = new Request(target.toString(), request);
    return fetch(proxiedRequest);
  }
};
