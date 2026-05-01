import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const BACKEND_BASE = (process.env.BACKEND_URL || process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

const IGNORE_HEADERS = new Set([
  "host", "connection", "keep-alive", "proxy-authenticate",
  "proxy-authorization", "te", "trailer", "transfer-encoding",
  "upgrade", "forwarded", "x-forwarded-host", "x-forwarded-proto",
  "x-forwarded-port", "x-vercel-",
]);

function sanitizeHeaderKey(k) {
  return k.toLowerCase().replace(/[^a-z0-9-]/g, "");
}

export default async function handler(req, res) {
  if (!BACKEND_BASE) {
    res.statusCode = 500;
    return res.end(JSON.stringify({ error: "misconfigured" }));
  }

  try {
    const targetUrl = BACKEND_BASE + req.url;
    
    const headers = {};
    let clientOriginIp = null;
    
    for (const [rawKey, rawValue] of Object.entries(req.headers)) {
      const lowerKey = rawKey.toLowerCase();
      let shouldSkip = false;
      for (const ig of IGNORE_HEADERS) {
        if (lowerKey === ig || lowerKey.startsWith(ig)) {
          shouldSkip = true;
          break;
        }
      }
      if (shouldSkip) continue;
      
      if (lowerKey === "x-real-ip") { clientOriginIp = rawValue; continue; }
      if (lowerKey === "x-forwarded-for") { if (!clientOriginIp) clientOriginIp = rawValue; continue; }
      
      const safeKey = sanitizeHeaderKey(lowerKey);
      const value = Array.isArray(rawValue) ? rawValue.join(", ") : rawValue;
      headers[safeKey] = value;
    }
    
    headers["x-original-ip"] = clientOriginIp || "0.0.0.0";
    headers["user-agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
    
    const method = req.method;
    const hasPayload = !["GET", "HEAD"].includes(method);
    
    const fetchOptions = { method, headers, redirect: "manual" };
    if (hasPayload) {
      fetchOptions.body = Readable.toWeb(req);
      fetchOptions.duplex = "half";
    }
    
    const upstream = await fetch(targetUrl, fetchOptions);
    
    res.statusCode = upstream.status;
    res.statusMessage = upstream.statusText;
    
    for (const [key, value] of upstream.headers) {
      const lower = key.toLowerCase();
      if (lower === "transfer-encoding" || lower === "content-length") continue;
      try {
        res.setHeader(sanitizeHeaderKey(key), value);
      } catch {}
    }
    
    if (upstream.body) {
      await pipeline(Readable.fromWeb(upstream.body), res);
    } else {
      res.end();
    }
  } catch (err) {
    console.error("bridge error:", err.message);
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end(JSON.stringify({ status: "unavailable" }));
    }
  }
}