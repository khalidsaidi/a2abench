## Public endpoint checks (UTC)
Sun Feb  1 06:04:58 AM UTC 2026

### GET https://a2abench-mcp.web.app/mcp
HTTP/2 302 
location: https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
content-type: text/plain; charset=utf-8
accept-ranges: bytes
date: Sun, 01 Feb 2026 06:05:01 GMT
x-served-by: cache-yvr1530-YVR
x-cache: MISS
x-cache-hits: 0
x-timer: S1769925902.585889,VS0,VE21
vary: x-fh-requested-host, accept-encoding
alt-svc: h3=":443";ma=86400,h3-29=":443";ma=86400,h3-27=":443";ma=86400
content-length: 79


--- body snippet ---
Redirecting to https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
### GET https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
HTTP/2 406 
content-type: application/json
x-cloud-trace-context: d0f0836c4d85c04999283ef9b53c51b5;o=1
date: Sun, 01 Feb 2026 06:05:01 GMT
server: Google Frontend
content-length: 116
alt-svc: h3=":443"; ma=2592000,h3-29=":443"; ma=2592000


--- body snippet ---
{"jsonrpc":"2.0","error":{"code":-32000,"message":"Not Acceptable: Client must accept text/event-stream"},"id":null}
### GET https://a2abench-api.web.app/.well-known/agent.json
{"name":"A2ABench","description":"Agent-native developer Q&A with REST + MCP + A2A discovery. Read-only endpoints do not require auth.","url":"https://a2abench-api.web.app","version":"0.1.8","protocolVersion":"0.1","skills":[{"id":"search","name":"Search","description":"Search questions by keyword or tag."},{"id":"fetch","name":"Fetch","description":"Fetch a question thread by id."}],"auth":{"type":"apiKey","description":"Read-only endpoints and MCP tools are public. Bearer API key for write endpoints. X-Admin-Token for admin endpoints."}}
### Glama connector status (best effort)
Attempted URLs:
https://glama.ai/mcp/connectors/io.github.khalidsaidi/a2abench
HTTP/2 103 
alt-svc: h3=":443"; ma=2592000
link: <https://static.glama.ai/client/fonts/inter/Inter-VariableFont.woff2>; rel=preload; as=font; crossorigin="anonymous", <https://static.glama.ai/client/assets/global-BCDvfCtW.css>; rel=preload; as=style

HTTP/2 103 
link: <https://static.glama.ai/client/assets/manifest-a0e128e8.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/entry.client-Dzdl-6uf.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/react-Cb6VpobZ.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/rolldown-runtime-CGo09zf6.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/root-DZ1buMi-.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/form-ClvuRYGh.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/NotFoundErrorDialog-CLFuXLb4.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/command-bar-DOgi9mJ1.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/_layout-BF7rFQnn.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/authentication-rXQ74euH.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/Modal-DVklh4Fn.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/SignUpModal-DhEDs0sX.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/ProgressIndicator--kGAv2Mr.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/icons-QhiJQPhh.js>; rel=preload; as=script; crossorigin=anonymous

HTTP/2 200 
accept-ch: Sec-CH-DPR, Sec-CH-Width, Sec-CH-Viewport-Width
content-security-policy: base-uri 'self'; connect-src 'self' https://*.google-analytics.com https://*.intercom.io https://*.intercomcdn.com https://*.intercomcdn.eu https://*.intercomusercontent.com https://*.sentry.io https://*.stripe.com/ https://accounts.google.com/gsi/ https://cdn.jsdelivr.net/ https://connect.facebook.net https://conversions-config.reddit.com/ https://maps.googleapis.com/ https://pixel-config.reddit.com https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com wss://*.intercom.io; default-src 'self' https://static.glama.ai/; font-src 'self' data: https://*.intercomcdn.com https://fonts.scalar.com/ https://static.glama.ai/; frame-src 'self' https://*.stripe.com/ https://accounts.google.com/gsi/ https://intercom-sheets.com https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com https://www.youtube.com; frame-ancestors 'self'; img-src 'self' blob: data: https://*.intercom-attachments-1.com https://*.intercom-attachments-2.com https://*.intercom-attachments-3.com https://*.intercom-attachments-4.com https://*.intercom-attachments-5.com https://*.intercom-attachments-6.com https://*.intercom-attachments-7.com https://*.intercom-attachments-8.com https://*.intercom-attachments-9.com https://*.intercom-attachments.eu https://*.intercom.io https://*.intercomassets.com https://*.intercomassets.eu https://*.intercomcdn.com https://*.intercomcdn.eu https://*.intercomusercontent.com https://*.reddit.com/ https://*.stripe.com https://glama.ai/uploads/ https://i.ytimg.com https://static.glama.ai/ https://www.facebook.com https://www.googletagmanager.com; media-src 'self' data: https://*.intercomcdn.com https://*.intercomcdn.eu https://static.glama.ai/; object-src 'none'; script-src 'nonce-c595522312473597bd6ac96d3eb1c96d' 'strict-dynamic' 'wasm-unsafe-eval' https://*.intercom.io https://*.stripe.com/ https://accounts.google.com/gsi/client https://connect.facebook.net https://googletagmanager.com https://js.intercomcdn.com https://maps.googleapis.com/ https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com; style-src 'self' 'unsafe-inline' https://accounts.google.com/gsi/style https://static.glama.ai/
--- body snippet ---
<!DOCTYPE html><html lang="en" data-sentry-component="Layout" data-sentry-source-file="root.tsx"><head><meta charSet="utf-8" data-sentry-element="meta" data-sentry-source-file="root.tsx"/><meta content="width=device-width, initial-scale=1" name="viewport" data-sentry-element="meta" data-sentry-source-file="root.tsx"/><link href="/manifest.json" rel="manifest"/><title>A2ABench - MCP Connector | Glama</title><meta content="Agent-native developer Q&A with REST, MCP, and A2A discovery endpoints." name="description"/><link href="https://glama.ai/mcp/connectors/io.github.khalidsaidi/a2abench" rel="canonical"/><meta content="A2ABench - MCP Connector" property="og:title"/><meta content="Agent-native developer Q&A with REST, MCP, and A2A discovery endpoints." property="og:description"/><meta content="https://glama.ai/logo.png" property="og:logo"/><meta content="https://glama.ai/generated-images/og?title=A2ABench+-+MCP+Connector" property="og:image"/><meta content="630" property="og:image:height"/><meta content="1200" property="og:image:width"/><meta content="website" property="og:type"/><meta content="https://glama.ai/mcp/connectors/io.github.khalidsaidi/a2abench" property="og:url"/><meta content="Glama – MCP Hosting Platform" property="og:site_name"/><meta content="summary_large_image" name="twitter:card"/><meta content="Glama – MCP Hosting Platform" name="twitter:title"/><meta content="Agent-native developer Q&A with REST, MCP, and A2A discovery endpoints." name="twitter:description"/><meta content="https://glama.ai/generated-images/og?title=A2ABench+-+MCP+Connector" name="twitter:image"/><meta content="630" name="twitter:image:height"/><meta content="1200" name="twitter:image:width"/><link as="style" href="https://static.glama.ai/client/assets/global-BCDvfCtW.css" rel="preload"/><link href="/favicon.ico" rel="icon" type="image/x-icon"/><link rel="modulepreload" href="https://static.glama.ai/client/entry.client-Dzdl-6uf.js"/><style nonce="c595522312473597bd6ac96d3eb1c96d">@font-face {
  font-display: optional;
  font-family: 'Inter';
  font-style: normal;
  font-weight: 100 900;
  src: url('https://static.glama.ai/client/fonts/inter/Inter-VariableFont.woff2') format('woff2');
}

body {
  font-family: 'Inter', Arial, sans-serif;

https://glama.ai/mcp/connector/io.github.khalidsaidi/a2abench
HTTP/2 103 
alt-svc: h3=":443"; ma=2592000
link: <https://static.glama.ai/client/fonts/inter/Inter-VariableFont.woff2>; rel=preload; as=font; crossorigin="anonymous", <https://static.glama.ai/client/assets/global-BCDvfCtW.css>; rel=preload; as=style

HTTP/2 103 
link: <https://static.glama.ai/client/assets/manifest-a0e128e8.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/entry.client-Dzdl-6uf.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/react-Cb6VpobZ.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/rolldown-runtime-CGo09zf6.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/root-DZ1buMi-.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/form-ClvuRYGh.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/NotFoundErrorDialog-CLFuXLb4.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/command-bar-DOgi9mJ1.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/_layout-BF7rFQnn.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/authentication-rXQ74euH.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/Modal-DVklh4Fn.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/SignUpModal-DhEDs0sX.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/ProgressIndicator--kGAv2Mr.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/icons-QhiJQPhh.js>; rel=preload; as=script; crossorigin=anonymous

HTTP/2 404 
accept-ch: Sec-CH-DPR, Sec-CH-Width, Sec-CH-Viewport-Width
content-security-policy: base-uri 'self'; connect-src 'self' https://*.google-analytics.com https://*.intercom.io https://*.intercomcdn.com https://*.intercomcdn.eu https://*.intercomusercontent.com https://*.sentry.io https://*.stripe.com/ https://accounts.google.com/gsi/ https://cdn.jsdelivr.net/ https://connect.facebook.net https://conversions-config.reddit.com/ https://maps.googleapis.com/ https://pixel-config.reddit.com https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com wss://*.intercom.io; default-src 'self' https://static.glama.ai/; font-src 'self' data: https://*.intercomcdn.com https://fonts.scalar.com/ https://static.glama.ai/; frame-src 'self' https://*.stripe.com/ https://accounts.google.com/gsi/ https://intercom-sheets.com https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com https://www.youtube.com; frame-ancestors 'self'; img-src 'self' blob: data: https://*.intercom-attachments-1.com https://*.intercom-attachments-2.com https://*.intercom-attachments-3.com https://*.intercom-attachments-4.com https://*.intercom-attachments-5.com https://*.intercom-attachments-6.com https://*.intercom-attachments-7.com https://*.intercom-attachments-8.com https://*.intercom-attachments-9.com https://*.intercom-attachments.eu https://*.intercom.io https://*.intercomassets.com https://*.intercomassets.eu https://*.intercomcdn.com https://*.intercomcdn.eu https://*.intercomusercontent.com https://*.reddit.com/ https://*.stripe.com https://glama.ai/uploads/ https://i.ytimg.com https://static.glama.ai/ https://www.facebook.com https://www.googletagmanager.com; media-src 'self' data: https://*.intercomcdn.com https://*.intercomcdn.eu https://static.glama.ai/; object-src 'none'; script-src 'nonce-fbd92e3bb24e3d0d58de47ee1d71ed43' 'strict-dynamic' 'wasm-unsafe-eval' https://*.intercom.io https://*.stripe.com/ https://accounts.google.com/gsi/client https://connect.facebook.net https://googletagmanager.com https://js.intercomcdn.com https://maps.googleapis.com/ https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com; style-src 'self' 'unsafe-inline' https://accounts.google.com/gsi/style https://static.glama.ai/
--- body snippet ---
<!DOCTYPE html><html lang="en" data-sentry-component="Layout" data-sentry-source-file="root.tsx"><head><meta charSet="utf-8" data-sentry-element="meta" data-sentry-source-file="root.tsx"/><meta content="width=device-width, initial-scale=1" name="viewport" data-sentry-element="meta" data-sentry-source-file="root.tsx"/><link href="/manifest.json" rel="manifest"/><link as="style" href="https://static.glama.ai/client/assets/global-BCDvfCtW.css" rel="preload"/><link href="/favicon.ico" rel="icon" type="image/x-icon"/><link rel="modulepreload" href="https://static.glama.ai/client/entry.client-Dzdl-6uf.js"/><style nonce="fbd92e3bb24e3d0d58de47ee1d71ed43">@font-face {
  font-display: optional;
  font-family: 'Inter';
  font-style: normal;
  font-weight: 100 900;
  src: url('https://static.glama.ai/client/fonts/inter/Inter-VariableFont.woff2') format('woff2');
}

body {
  font-family: 'Inter', Arial, sans-serif;

https://glama.ai/mcp/connectors
HTTP/2 103 
alt-svc: h3=":443"; ma=2592000
link: <https://static.glama.ai/client/fonts/inter/Inter-VariableFont.woff2>; rel=preload; as=font; crossorigin="anonymous", <https://static.glama.ai/client/assets/global-BCDvfCtW.css>; rel=preload; as=style

HTTP/2 103 
link: <https://static.glama.ai/client/assets/manifest-a0e128e8.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/entry.client-Dzdl-6uf.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/react-Cb6VpobZ.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/rolldown-runtime-CGo09zf6.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/root-DZ1buMi-.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/form-ClvuRYGh.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/NotFoundErrorDialog-CLFuXLb4.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/command-bar-DOgi9mJ1.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/_layout-BF7rFQnn.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/authentication-rXQ74euH.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/Modal-DVklh4Fn.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/SignUpModal-DhEDs0sX.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/ProgressIndicator--kGAv2Mr.js>; rel=preload; as=script; crossorigin=anonymous, <https://static.glama.ai/client/icons-QhiJQPhh.js>; rel=preload; as=script; crossorigin=anonymous

HTTP/2 200 
accept-ch: Sec-CH-DPR, Sec-CH-Width, Sec-CH-Viewport-Width
content-security-policy: base-uri 'self'; connect-src 'self' https://*.google-analytics.com https://*.intercom.io https://*.intercomcdn.com https://*.intercomcdn.eu https://*.intercomusercontent.com https://*.sentry.io https://*.stripe.com/ https://accounts.google.com/gsi/ https://cdn.jsdelivr.net/ https://connect.facebook.net https://conversions-config.reddit.com/ https://maps.googleapis.com/ https://pixel-config.reddit.com https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com wss://*.intercom.io; default-src 'self' https://static.glama.ai/; font-src 'self' data: https://*.intercomcdn.com https://fonts.scalar.com/ https://static.glama.ai/; frame-src 'self' https://*.stripe.com/ https://accounts.google.com/gsi/ https://intercom-sheets.com https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com https://www.youtube.com; frame-ancestors 'self'; img-src 'self' blob: data: https://*.intercom-attachments-1.com https://*.intercom-attachments-2.com https://*.intercom-attachments-3.com https://*.intercom-attachments-4.com https://*.intercom-attachments-5.com https://*.intercom-attachments-6.com https://*.intercom-attachments-7.com https://*.intercom-attachments-8.com https://*.intercom-attachments-9.com https://*.intercom-attachments.eu https://*.intercom.io https://*.intercomassets.com https://*.intercomassets.eu https://*.intercomcdn.com https://*.intercomcdn.eu https://*.intercomusercontent.com https://*.reddit.com/ https://*.stripe.com https://glama.ai/uploads/ https://i.ytimg.com https://static.glama.ai/ https://www.facebook.com https://www.googletagmanager.com; media-src 'self' data: https://*.intercomcdn.com https://*.intercomcdn.eu https://static.glama.ai/; object-src 'none'; script-src 'nonce-d568f23d5f67c3cb82bb9e416c8b9dcf' 'strict-dynamic' 'wasm-unsafe-eval' https://*.intercom.io https://*.stripe.com/ https://accounts.google.com/gsi/client https://connect.facebook.net https://googletagmanager.com https://js.intercomcdn.com https://maps.googleapis.com/ https://static.glama.ai/ https://www.facebook.com https://www.redditstatic.com; style-src 'self' 'unsafe-inline' https://accounts.google.com/gsi/style https://static.glama.ai/
--- body snippet ---
<!DOCTYPE html><html lang="en" data-sentry-component="Layout" data-sentry-source-file="root.tsx"><head><meta charSet="utf-8" data-sentry-element="meta" data-sentry-source-file="root.tsx"/><meta content="width=device-width, initial-scale=1" name="viewport" data-sentry-element="meta" data-sentry-source-file="root.tsx"/><link href="/manifest.json" rel="manifest"/><title>MCP Connectors | Glama</title><meta content="Browse MCP Connectors from the official MCP Registry. Connect to these servers directly without local installation." name="description"/><link href="https://glama.ai/mcp/connectors" rel="canonical"/><meta content="MCP Connectors" property="og:title"/><meta content="Browse MCP Connectors from the official MCP Registry. Connect to these servers directly without local installation." property="og:description"/><meta content="https://glama.ai/logo.png" property="og:logo"/><meta content="https://glama.ai/generated-images/og?title=MCP+Connectors" property="og:image"/><meta content="630" property="og:image:height"/><meta content="1200" property="og:image:width"/><meta content="website" property="og:type"/><meta content="https://glama.ai/mcp/connectors" property="og:url"/><meta content="Glama – MCP Hosting Platform" property="og:site_name"/><meta content="summary_large_image" name="twitter:card"/><meta content="Glama – MCP Hosting Platform" name="twitter:title"/><meta content="Browse MCP Connectors from the official MCP Registry. Connect to these servers directly without local installation." name="twitter:description"/><meta content="https://glama.ai/generated-images/og?title=MCP+Connectors" name="twitter:image"/><meta content="630" name="twitter:image:height"/><meta content="1200" name="twitter:image:width"/><link as="style" href="https://static.glama.ai/client/assets/global-BCDvfCtW.css" rel="preload"/><link href="/favicon.ico" rel="icon" type="image/x-icon"/><link rel="modulepreload" href="https://static.glama.ai/client/entry.client-Dzdl-6uf.js"/><style nonce="d568f23d5f67c3cb82bb9e416c8b9dcf">@font-face {
  font-display: optional;
  font-family: 'Inter';
  font-style: normal;
  font-weight: 100 900;
  src: url('https://static.glama.ai/client/fonts/inter/Inter-VariableFont.woff2') format('woff2');
}

body {
  font-family: 'Inter', Arial, sans-serif;

### Root-cause hypothesis
The Cloud Run MCP endpoint currently responds to GET /mcp with HTTP 406 when the client does not explicitly accept `text/event-stream`, which likely causes Glama inspector and human/browser checks to mark the connector unhealthy. The Firebase host is correctly redirecting to Cloud Run (302), but because the destination GET /mcp returns 406, the discovery funnel still looks broken. Aligning GET/HEAD/OPTIONS /mcp to return 200 with a friendly payload (and ensuring MCP POST remains compliant) should eliminate the 406 and improve directory/inspector health.

## Post-fix checks (UTC)
Sun Feb  1 06:29:49 AM UTC 2026

### GET https://a2abench-mcp.web.app/mcp
HTTP/2 301 
location: https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
content-type: text/plain; charset=utf-8
accept-ranges: bytes
date: Sun, 01 Feb 2026 06:29:50 GMT
x-served-by: cache-yvr1524-YVR
x-cache: MISS
x-cache-hits: 0
x-timer: S1769927391.594422,VS0,VE20
vary: x-fh-requested-host, accept-encoding
alt-svc: h3=":443";ma=86400,h3-29=":443";ma=86400,h3-27=":443";ma=86400
content-length: 79


--- body snippet ---
Redirecting to https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
### GET https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
HTTP/2 200 
content-type: text/plain; charset=utf-8
x-cloud-trace-context: a1e09b7796ef1a29469df3e609878ec4;o=1
date: Sun, 01 Feb 2026 06:29:50 GMT
server: Google Frontend
content-length: 202
alt-svc: h3=":443"; ma=2592000,h3-29=":443"; ma=2592000


--- body snippet ---
A2ABench MCP endpoint. Use an MCP client.
Endpoint: https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
Docs: https://a2abench-api.web.app/docs
Repo: https://github.com/khalidsaidi/a2abench
### GET https://a2abench-mcp-remote-405318049509.us-central1.run.app/healthz
HTTP/2 404 
content-type: text/html; charset=UTF-8
referrer-policy: no-referrer
content-length: 1568
date: Sun, 01 Feb 2026 06:29:50 GMT
alt-svc: h3=":443"; ma=2592000,h3-29=":443"; ma=2592000


--- body snippet ---
<!DOCTYPE html>
<html lang=en>
  <meta charset=utf-8>
  <meta name=viewport content="initial-scale=1, minimum-scale=1, width=device-width">
  <title>Error 404 (Not Found)!!1</title>
  <style>
    *{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px}
  </style>
  <a href=//www.google.com/><span id=logo aria-label=Google></span></a>
  <p><b>404.</b> <ins>That’s an error.</ins>
  <p>The requested URL <code>/healthz</code> was not found on this server.  <ins>That’s all we know.</ins>

### GET https://a2abench-api.web.app/.well-known/agent.json
{"name":"A2ABench","description":"Agent-native developer Q&A with REST + MCP + A2A discovery. Read-only endpoints do not require auth.","url":"https://a2abench-api.web.app","version":"0.1.12","protocolVersion":"0.1","skills":[{"id":"search","name":"Search","description":"Search questions by keyword or tag."},{"id":"fetch","name":"Fetch","description":"Fetch a question thread by id."}],"auth":{"type":"apiKey","description":"Read-only endpoints and MCP tools are public. Bearer API key for write endpoints. X-Admin-Token for admin endpoints."}}
### GET https://a2abench-mcp-remote-405318049509.us-central1.run.app/healthz/
HTTP/2 200 
content-type: application/json
x-cloud-trace-context: 5364a3357e90e1affcac81510d1141a2;o=1
date: Sun, 01 Feb 2026 06:36:06 GMT
server: Google Frontend
content-length: 104
alt-svc: h3=":443"; ma=2592000,h3-29=":443"; ma=2592000


--- body snippet ---
{"status":"ok","version":"0.1.12","commit":"5a20ccaad155df2ccf19c3ddea4928a48784919c","uptimeMs":657012}