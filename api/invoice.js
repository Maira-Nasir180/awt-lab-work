/**
 * Serverless function (Vercel) - PDF invoice generator.
 * Cold start detection: module scope runs ONCE per container. The first invocation of a
 * freshly started container is a COLD start; later invocations reuse it (WARM).
 */
const PDFDocument = require('pdfkit');

const containerBootedAt = Date.now();
const containerId = Math.random().toString(36).slice(2, 10);
let invocations = 0;

function buildPdf({ orderId, tenantSlug, customerName, total, items }) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 50 });
    const chunks = [];
    doc.on('data', (c) => chunks.push(c));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
    doc.fontSize(22).text('INVOICE', { align: 'right' });
    doc.moveDown().fontSize(12).text(`Tenant: ${tenantSlug}`);
    doc.text(`Order: ${orderId}`);
    doc.text(`Customer: ${customerName}`);
    doc.text(`Date: ${new Date().toISOString().slice(0, 10)}`);
    doc.moveDown().text('Items', { underline: true });
    for (const it of items || []) {
      doc.text(`${it.productId.slice(0, 8)}  x${it.quantity}  @ ${Number(it.unitPrice).toFixed(2)}  =  ${(it.quantity * it.unitPrice).toFixed(2)}`);
    }
    doc.moveDown().fontSize(14).text(`TOTAL: ${Number(total).toFixed(2)}`, { align: 'right' });
    doc.end();
  });
}

module.exports = async (req, res) => {
  const t0 = process.hrtime.bigint();
  const coldStart = invocations === 0;
  invocations += 1;

  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });
  if (!process.env.INVOICE_SECRET || req.headers['x-invoice-secret'] !== process.env.INVOICE_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    if (!body || !body.orderId || !Array.isArray(body.items)) return res.status(400).json({ error: 'orderId and items required' });
    const pdf = await buildPdf(body);
    const durationMs = Number(process.hrtime.bigint() - t0) / 1e6;
    const out = {
      orderId: body.orderId, coldStart, durationMs: Number(durationMs.toFixed(2)),
      containerId, invocationNo: invocations, containerAgeMs: Date.now() - containerBootedAt,
      pdfBytes: pdf.length, pdfBase64: pdf.toString('base64'),
    };
    console.log(JSON.stringify({ msg: 'invoice', coldStart, durationMs: out.durationMs, containerId, invocationNo: invocations }));
    res.setHeader('x-cold-start', String(coldStart));
    res.status(200).json(out);
  } catch (e) {
    console.error('invoice error', e);
    res.status(500).json({ error: 'invoice generation failed' });
  }
};
