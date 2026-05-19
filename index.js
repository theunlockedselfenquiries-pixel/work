import express from "express";
import { Resend } from "resend";
import cron from "node-cron";
import { create } from "@wppconnect-team/wppconnect";
import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, DeleteObjectsCommand } from "@aws-sdk/client-s3";
import { existsSync, mkdirSync } from "fs";

const app = express();
app.use(express.json());

const resend = new Resend(process.env.RESEND_API_KEY);

const {
  TO_EMAIL,
  FROM_EMAIL,
  TARGET_GROUP_NAME,        // Partial name of the WhatsApp group to monitor
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET_NAME,
  TRIGGER_SECRET,
} = process.env;

// ── R2 client ─────────────────────────────────────────────────────────────────
const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

// ── Helpers ───────────────────────────────────────────────────────────────────
function getWeekKey() {
  const now = new Date();
  const year = now.getUTCFullYear();
  const startOfYear = new Date(Date.UTC(year, 0, 1));
  const week = Math.ceil(((now - startOfYear) / 86400000 + startOfYear.getUTCDay() + 1) / 7);
  return `${year}-W${String(week).padStart(2, "0")}`;
}

async function storeReceipt({ imageId, imageBuffer, mimeType, sender, timestamp, caption }) {
  const ext = mimeType.split("/")[1]?.split(";")[0] || "jpg";
  const week = getWeekKey();
  const imageKey = `receipts/${week}/${imageId}.${ext}`;
  const metaKey = `receipts/${week}/${imageId}.json`;

  await r2.send(new PutObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: imageKey,
    Body: imageBuffer,
    ContentType: mimeType,
  }));

  await r2.send(new PutObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: metaKey,
    Body: JSON.stringify({ sender, timestamp, caption, imageKey, mimeType }),
    ContentType: "application/json",
  }));

  console.log(`Stored receipt ${imageId} for week ${week}`);
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf-8");
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function getWeekReceipts(week) {
  const prefix = `receipts/${week}/`;
  const listed = await r2.send(new ListObjectsV2Command({ Bucket: R2_BUCKET_NAME, Prefix: prefix }));
  const keys = (listed.Contents || []).map(o => o.Key);
  const metaKeys = keys.filter(k => k.endsWith(".json"));

  const receipts = [];
  for (const metaKey of metaKeys) {
    const metaObj = await r2.send(new GetObjectCommand({ Bucket: R2_BUCKET_NAME, Key: metaKey }));
    const meta = JSON.parse(await streamToString(metaObj.Body));
    const imgObj = await r2.send(new GetObjectCommand({ Bucket: R2_BUCKET_NAME, Key: meta.imageKey }));
    const imgBuffer = await streamToBuffer(imgObj.Body);
    receipts.push({ ...meta, imgBuffer });
  }

  return { receipts, allKeys: keys };
}

async function deleteWeekReceipts(allKeys) {
  if (!allKeys.length) return;
  await r2.send(new DeleteObjectsCommand({
    Bucket: R2_BUCKET_NAME,
    Delete: { Objects: allKeys.map(Key => ({ Key })) },
  }));
}

// ── Weekly digest ─────────────────────────────────────────────────────────────
async function sendWeeklyDigest() {
  const week = getWeekKey();
  console.log(`Sending weekly digest for ${week}`);

  const { receipts, allKeys } = await getWeekReceipts(week);

  if (!receipts.length) {
    console.log("No receipts this week — skipping email.");
    return;
  }

  const attachments = receipts.map((r, i) => {
    const ext = r.mimeType.split("/")[1]?.split(";")[0] || "jpg";
    return {
      filename: `receipt-${i + 1}-${r.timestamp.replace(/[^a-z0-9]/gi, "-")}.${ext}`,
      content: r.imgBuffer.toString("base64"),
    };
  });

  const receiptRows = receipts.map((r, i) => `
    <tr>
      <td style="padding:8px;border-bottom:1px solid #eee">${i + 1}</td>
      <td style="padding:8px;border-bottom:1px solid #eee">${r.timestamp}</td>
      <td style="padding:8px;border-bottom:1px solid #eee">${r.sender}</td>
      <td style="padding:8px;border-bottom:1px solid #eee">${r.caption || "—"}</td>
    </tr>`).join("");

  await resend.emails.send({
    from: FROM_EMAIL,
    to: TO_EMAIL,
    subject: `📄 Weekly Receipts — ${week} (${receipts.length} image${receipts.length > 1 ? "s" : ""})`,
    html: `
      <h2>Weekly Receipt Summary — ${week}</h2>
      <p>${receipts.length} receipt(s) received this week.</p>
      <table style="border-collapse:collapse;width:100%;font-family:sans-serif;font-size:14px">
        <thead>
          <tr style="background:#f5f5f5">
            <th style="padding:8px;text-align:left">#</th>
            <th style="padding:8px;text-align:left">Time</th>
            <th style="padding:8px;text-align:left">From</th>
            <th style="padding:8px;text-align:left">Caption</th>
          </tr>
        </thead>
        <tbody>${receiptRows}</tbody>
      </table>
      <p style="margin-top:16px;color:#666;font-size:13px">All images are attached.</p>
    `,
    attachments,
  });

  console.log(`Digest sent with ${receipts.length} receipts. Cleaning up R2...`);
  await deleteWeekReceipts(allKeys);
}

// ── WPPConnect ────────────────────────────────────────────────────────────────
async function startWhatsApp() {
  const sessionDir = "/tmp/wpp-session";
  if (!existsSync(sessionDir)) mkdirSync(sessionDir, { recursive: true });

  const client = await create({
    session: "receipt-bot",
    folderNameToken: sessionDir,
    headless: true,
    logQR: true,           // QR prints to Render logs — scan on first run
    disableWelcome: true,
    updatesLog: false,
    autoClose: 0,
    puppeteerOptions: {
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    },
  });

  client.onMessage(async (message) => {
    try {
      if (!message.isGroupMsg) return;
      if (message.type !== "image") return;

      // Filter to target group if env var is set
      if (
        TARGET_GROUP_NAME &&
        !message.chat?.name?.toLowerCase().includes(TARGET_GROUP_NAME.toLowerCase())
      ) return;

      const sender = message.sender?.pushname || message.from;
      console.log(`Image received in "${message.chat?.name}" from ${sender}`);

      const imageBuffer = await client.decryptFile(message);
      const mimeType = message.mimetype || "image/jpeg";
      const imageId = message.id || Date.now().toString();
      const timestamp = new Date(message.timestamp * 1000).toLocaleString();
      const caption = message.caption || "No caption";

      await storeReceipt({ imageId, imageBuffer, mimeType, sender, timestamp, caption });

    } catch (err) {
      console.error("Error handling message:", err.message);
    }
  });

  console.log("WPPConnect running — waiting for messages...");
  return client;
}

// ── Keep-alive ping for UptimeRobot ──────────────────────────────────────────
app.get("/ping", (req, res) => res.send("ok"));

// ── Manual digest trigger ─────────────────────────────────────────────────────
app.post("/send-digest-now", async (req, res) => {
  if (req.headers["x-trigger-secret"] !== TRIGGER_SECRET) return res.sendStatus(401);
  try {
    await sendWeeklyDigest();
    res.json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── Cron: every Friday at 6pm UTC ────────────────────────────────────────────
cron.schedule("0 18 * * 5", () => {
  sendWeeklyDigest().catch(err => console.error("Digest cron failed:", err));
});

// ── Boot ──────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`HTTP server on port ${PORT}`));
startWhatsApp().catch(err => {
  console.error("WPPConnect failed to start:", err);
  process.exit(1);
});
