require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const { pool } = require('./config/db');
const { resolveTenant } = require('./middleware/tenant');
const { requireAuth } = require('./middleware/auth');

if (!process.env.JWT_SECRET) { console.error('JWT_SECRET is required'); process.exit(1); }

const app = express();
app.use(cors());
app.use(express.json({ limit: '100kb' }));
app.use(express.static(path.join(__dirname, '..', 'public')));

app.get('/health', async (req, res) => {
  try { await pool.query('SELECT 1'); res.json({ status: 'ok' }); }
  catch (e) { res.status(500).json({ status: 'db_down' }); }
});

const api = express.Router();
api.use(resolveTenant);                                   // 1) who is the tenant?
api.use('/auth', require('./routes/auth'));               // login (tenant-scoped)
api.use(requireAuth);                                     // 2) everything below needs a JWT of THAT tenant
api.use('/products', require('./routes/products'));
api.use('/orders', require('./routes/orders'));
api.use('/invoices', require('./routes/invoices'));
app.use('/api/v1', api);

app.use((req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, req, res, next) => {
  const status = err.status || 500;
  if (status >= 500) console.error('[error]', err);
  res.status(status).json({ error: status >= 500 ? 'Internal server error' : err.message });
});

if (require.main === module) {
  const port = process.env.PORT || 3000;
  const server = app.listen(port, () => console.log(`API listening on :${port}`));
  const stop = async () => { server.close(); await pool.end(); process.exit(0); };
  process.on('SIGTERM', stop); process.on('SIGINT', stop);
}
module.exports = app;
