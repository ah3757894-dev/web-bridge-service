import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const TARGET = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

const SKIP_HEADERS = new Set([
  "host", "connection", "keep-alive", "proxy-authenticate",
  "proxy-authorization", "te", "trailer", "transfer-encoding",
  "upgrade", "forwarded", "x-forwarded-host", "x-forwarded-proto",
  "x-forwarded-port",
]);

export default async function handler(req, res) {
  if (!TARGET) {
    res.statusCode = 500;
    return res.end("Missing configuration");
  }

  try {
    const url = TARGET + req.url;
    const headers = {};
    let ip = null;
    
    for (const key of Object.keys(req.headers)) {
      const k = key.toLowerCase();
      const v = req.headers[key];
      if (SKIP_HEADERS.has(k)) continue;
      if (k.startsWith("x-vercel-")) continue;
      if (k === "x-real-ip") { ip = v; continue; }
      if (k === "x-forwarded-for") { if (!ip) ip = v; continue; }
      headers[k] = Array.isArray(v) ? v.join(", ") : v;
    }
    if (ip) headers["x-forwarded-for"] = ip;
    
    const method = req.method;
    const hasBody = method !== "GET" && method !== "HEAD";
    const opts = { method, headers, redirect: "manual" };
    
    if (hasBody) {
      opts.body = Readable.toWeb(req);
      opts.duplex = "half";
    }
    
    const upstream = await fetch(url, opts);
    res.statusCode = upstream.status;
    
    for (const [k, v] of upstream.headers) {
      if (k.toLowerCase() === "transfer-encoding") continue;
      try { res.setHeader(k, v); } catch {}
    }
    
    if (upstream.body) {
      await pipeline(Readable.fromWeb(upstream.body), res);
    } else {
      res.end();
    }
  } catch (err) {
    console.error("error:", err);
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway");
    }
  }
}
