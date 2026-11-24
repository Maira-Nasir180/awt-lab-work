const jwt = require('jsonwebtoken');
const { pool } = require('../config/db');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const cache = new Map(); // key -> { tenant, exp }
const TTL = 60_000;

async function findTenant(key) {
  const hit = cache.get(key);
  if (hit && hit.exp > Date.now()) return hit.tenant;
  const { rows } = await pool.query(
    UUID_RE.test(key)
      ? 'SELECT id, slug, name FROM tenants WHERE id = $1'
      : 'SELECT id, slug, name FROM tenants WHERE slug = $1',
    [key.toLowerCase()]
  );
  const tenant = rows[0] || null;
  if (tenant) cache.set(key, { tenant, exp: Date.now() + TTL });
  return tenant;
}

function fromSubdomain(req) {
  const base = process.env.BASE_DOMAIN; // e.g. "myapp.com" => acme.myapp.com
  if (!base) return null;
  const host = (req.headers.host || '').split(':')[0];
  if (host.endsWith('.' + base)) return host.slice(0, -(base.length + 1));
  return null;
}

/**
 * Tenant Resolution Middleware.
 * Order: JWT claim -> x-tenant-id header -> subdomain.
 * If a JWT and a header are BOTH present and disagree => 403 (prevents header spoofing).
 */
async function resolveTenant(req, res, next) {
  try {
    let jwtTenant = null;
    const auth = req.headers.authorization || '';
    if (auth.startsWith('Bearer ')) {
      try {
        const payload = jwt.verify(auth.slice(7), process.env.JWT_SECRET);
        jwtTenant = payload.tenant_id;
      } catch (_) { /* auth middleware will reject invalid tokens */ }
    }
    const headerTenant = req.headers['x-tenant-id'];
    const key = jwtTenant || headerTenant || fromSubdomain(req);
    if (!key) return res.status(400).json({ error: 'Tenant not specified (send x-tenant-id header, JWT, or use a tenant subdomain)' });

    const tenant = await findTenant(String(key));
    if (!tenant) return res.status(404).json({ error: 'Unknown tenant' });

    if (jwtTenant && headerTenant) {
      const headerTenantRow = await findTenant(String(headerTenant));
      if (!headerTenantRow || headerTenantRow.id !== tenant.id) {
        return res.status(403).json({ error: 'Tenant mismatch between token and x-tenant-id header' });
      }
    }
    req.tenant = tenant;
    next();
  } catch (e) { next(e); }
}

module.exports = { resolveTenant };
