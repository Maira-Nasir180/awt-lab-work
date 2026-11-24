require('dotenv').config();
const bcrypt = require('bcryptjs');
const { pool, withTenant } = require('../src/config/db');

const TENANTS = [
  { slug: 'acme', name: 'Acme Corporation', email: 'admin@acme.com', products: [
    ['ACME-001', 'Rocket Skates', 120.0, 500], ['ACME-002', 'Anvil (1 ton)', 80.5, 300],
    ['ACME-003', 'Giant Magnet', 45.0, 800], ['ACME-004', 'Portable Hole', 15.25, 1000], ['ACME-005', 'Jet Pack', 899.99, 100] ] },
  { slug: 'globex', name: 'Globex Industries', email: 'admin@globex.com', products: [
    ['GLX-100', 'Quantum Widget', 25.0, 600], ['GLX-200', 'Hyper Sprocket', 60.0, 400],
    ['GLX-300', 'Flux Capacitor', 1500.0, 50], ['GLX-400', 'Neutrino Filter', 12.75, 900], ['GLX-500', 'Plasma Coupler', 210.0, 150] ] },
];
const PASSWORD = 'Password123!';

(async () => {
  const hash = await bcrypt.hash(PASSWORD, 10);
  for (const t of TENANTS) {
    const { rows: [tenant] } = await pool.query(
      `INSERT INTO tenants (slug, name) VALUES ($1,$2)
       ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name RETURNING id`, [t.slug, t.name]);
    await withTenant(tenant.id, async (db) => {
      await db.query(
        `INSERT INTO users (tenant_id, email, password_hash, role) VALUES ($1,$2,$3,'admin')
         ON CONFLICT (tenant_id, email) DO UPDATE SET password_hash = EXCLUDED.password_hash`,
        [tenant.id, t.email, hash]);
      for (const [sku, name, price, stock] of t.products) {
        await db.query(
          `INSERT INTO products (tenant_id, sku, name, price, stock) VALUES ($1,$2,$3,$4,$5)
           ON CONFLICT (tenant_id, sku) DO NOTHING`, [tenant.id, sku, name, price, stock]);
      }
    });
    console.log(`Seeded tenant ${t.slug}  ->  ${t.email} / ${PASSWORD}`);
  }
  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
