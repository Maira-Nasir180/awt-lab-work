const router = require('express').Router();
const { withTenant } = require('../config/db');

router.get('/', async (req, res, next) => {
  try {
    const rows = await withTenant(req.tenant.id, async (db) =>
      (await db.query('SELECT id, sku, name, price, stock FROM products WHERE tenant_id = $1 ORDER BY name', [req.tenant.id])).rows);
    res.json({ tenant: req.tenant.slug, data: rows });
  } catch (e) { next(e); }
});

module.exports = router;
