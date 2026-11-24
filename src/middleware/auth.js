const jwt = require('jsonwebtoken');

function requireAuth(req, res, next) {
  const auth = req.headers.authorization || '';
  if (!auth.startsWith('Bearer ')) return res.status(401).json({ error: 'Missing bearer token' });
  try {
    const payload = jwt.verify(auth.slice(7), process.env.JWT_SECRET);
    // The token must belong to the tenant that the request resolved to.
    if (payload.tenant_id !== req.tenant.id) return res.status(403).json({ error: 'Token does not belong to this tenant' });
    req.user = { id: payload.sub, email: payload.email, role: payload.role };
    next();
  } catch (_) {
    res.status(401).json({ error: 'Invalid or expired token' });
  }
}

module.exports = { requireAuth };
