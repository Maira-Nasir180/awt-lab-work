// End-to-end checks incl. tenant isolation. usage: BASE_URL=http://localhost:3000 npm run smoke
const BASE = process.env.BASE_URL || 'http://localhost:3000';
let pass = 0, fail = 0;
const ok = (name, cond, extra = '') => { cond ? pass++ : fail++; console.log(`${cond ? 'PASS' : 'FAIL'}  ${name} ${cond ? '' : extra}`); };
async function call(method, path, { token, tenant, body } = {}) {
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = 'Bearer ' + token;
  if (tenant) headers['x-tenant-id'] = tenant;
  const r = await fetch(BASE + path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  let j = null; try { j = await r.json(); } catch (_) {}
  return { status: r.status, body: j };
}
(async () => {
  const a = await call('POST', '/api/v1/auth/login', { tenant: 'acme', body: { email: 'admin@acme.com', password: 'Password123!' } });
  const b = await call('POST', '/api/v1/auth/login', { tenant: 'globex', body: { email: 'admin@globex.com', password: 'Password123!' } });
  ok('Tenant A login', a.status === 200 && a.body.token); ok('Tenant B login', b.status === 200 && b.body.token);
  const tokA = a.body.token, tokB = b.body.token;

  ok('Cross-tenant login blocked (A user on B tenant)', (await call('POST', '/api/v1/auth/login', { tenant: 'globex', body: { email: 'admin@acme.com', password: 'Password123!' } })).status === 401);
  ok('JWT alone resolves tenant (no header needed)', (await call('GET', '/api/v1/products', { token: tokA })).status === 200);
  ok('No tenant + no token => 400', (await call('GET', '/api/v1/products')).status === 400);
  ok('No token => 401', (await call('GET', '/api/v1/products', { tenant: 'acme' })).status === 401);
  ok('Unknown tenant => 404', (await call('GET', '/api/v1/products', { tenant: 'nope' })).status === 404);
  ok('Token A + header B => 403 (spoof blocked)', (await call('GET', '/api/v1/products', { token: tokA, tenant: 'globex' })).status === 403);

  const pa = (await call('GET', '/api/v1/products', { token: tokA })).body.data;
  const pb = (await call('GET', '/api/v1/products', { token: tokB })).body.data;
  ok('A sees only ACME products', pa.length > 0 && pa.every((p) => p.sku.startsWith('ACME')));
  ok('B sees only GLX products', pb.length > 0 && pb.every((p) => p.sku.startsWith('GLX')));

  const o = await call('POST', '/api/v1/orders', { token: tokA, body: { customerName: 'Smoke Test', items: [{ productId: pa[0].id, quantity: 2 }, { productId: pa[1].id, quantity: 1 }] } });
  ok('POST /orders => 202 Accepted', o.status === 202 && o.body.orderId, JSON.stringify(o.body));
  const oid = o.body.orderId;
  ok('A can read own order', (await call('GET', '/api/v1/orders/' + oid, { token: tokA })).status === 200);
  ok('B cannot read A order (404)', (await call('GET', '/api/v1/orders/' + oid, { token: tokB })).status === 404);
  ok('B cannot order A product (422)', (await call('POST', '/api/v1/orders', { token: tokB, body: { customerName: 'x', items: [{ productId: pa[0].id, quantity: 1 }] } })).status === 422);
  const listB = (await call('GET', '/api/v1/orders', { token: tokB })).body;
  ok('B order list does not contain A order', !listB.data.some((x) => x.id === oid));
  ok('Validation => 400', (await call('POST', '/api/v1/orders', { token: tokA, body: { customerName: 'x', items: [] } })).status === 400);

  const slow = (await call('GET', '/api/v1/orders/slow?limit=50', { token: tokA })).body;
  const fast = (await call('GET', '/api/v1/orders?limit=50', { token: tokA })).body;
  ok(`N+1 issues ${slow.queryCount} queries, JOIN issues ${fast.queryCount}`, slow.queryCount > 10 && fast.queryCount === 1);
  ok('Both strategies return same order ids', JSON.stringify(slow.data.map((x) => x.id)) === JSON.stringify(fast.data.map((x) => x.id)));
  console.log(`   N+1: ${slow.durationMs.toFixed(1)} ms | JOIN: ${fast.durationMs.toFixed(1)} ms`);
  console.log(`\n${pass} passed, ${fail} failed`); process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
