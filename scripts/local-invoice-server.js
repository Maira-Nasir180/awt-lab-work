// Runs the Vercel function locally on :3001 (same code, so cold/warm detection works the same way).
require('dotenv').config();
const http = require('http');
const handler = require('../api/invoice');
http.createServer((req, res) => {
  if (req.url !== '/api/invoice') { res.statusCode = 404; return res.end(); }
  let raw = '';
  req.on('data', (c) => (raw += c));
  req.on('end', () => {
    try { req.body = raw ? JSON.parse(raw) : {}; } catch { req.body = null; }
    res.status = (c) => { res.statusCode = c; return res; };
    res.json = (o) => { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(o)); };
    handler(req, res);
  });
}).listen(3001, () => console.log('local invoice function on :3001'));
