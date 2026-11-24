const router = require('express').Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { withTenant } = require('../config/db');

router.post('/login', async (req, res, next) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });
    const user = await withTenant(req.tenant.id, async (db) => {
      const { rows } = await db.query(
        'SELECT id, email, password_hash, role FROM users WHERE tenant_id = $1 AND email = $2',
        [req.tenant.id, String(email).toLowerCase()]
      );
      return rows[0];
    });
    if (!user || !(await bcrypt.compare(password, user.password_hash))) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    const token = jwt.sign(
      { sub: user.id, email: user.email, role: user.role, tenant_id: req.tenant.id },
      process.env.JWT_SECRET, { expiresIn: '8h' }
    );
    res.json({ token, tenant: req.tenant, user: { id: user.id, email: user.email, role: user.role } });
  } catch (e) { next(e); }
});

module.exports = router;
