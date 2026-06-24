/**
 * test-with-deepgram.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Reliability test: connects your live /stream output to Deepgram's
 * real-time transcription API (free $200 credit, no credit card to start).
 *
 * This proves the FULL pipeline end-to-end:
 *
 *   MeetStream bot → your server → /stream → Deepgram → live transcript
 *
 * If you see transcripts printing here, your real-time audio pipeline
 * is verified working — independent confirmation from a third-party service,
 * separate from MeetStream's own transcript webhook.
 *
 * ── Setup ──────────────────────────────────────────────────────────────────
 * 1. Get a free Deepgram API key: https://console.deepgram.com/signup
 *    (Free $200 credit, no card required to start)
 * 2. Add to your .env:
 *      DEEPGRAM_API_KEY=your_key_here
 * 3. Run your main server in one terminal:
 *      npm start
 * 4. Run this script in a second terminal:
 *      node test-with-deepgram.js
 * 5. Speak in the meeting — transcripts appear here within ~1 second.
 *
 * ── How it works ──────────────────────────────────────────────────────────
 * - Connects to YOUR /stream WebSocket (the one MeetStream audio flows into)
 * - Parses each frame's speaker name + PCM payload (same envelope as
 *   consumer-example.js)
 * - Forwards the raw PCM straight through to Deepgram's streaming endpoint
 * - Deepgram sends back transcripts in real time, which we print here
 *
 * No audio is buffered, transcoded, or modified — it is forwarded as-is,
 * which doubles as proof that the PCM16/48kHz/mono format is correct,
 * since Deepgram will immediately disconnect on malformed audio.
 */

import "dotenv/config";
import { WebSocket } from "ws";
import chalk from "chalk";

const LOCAL_STREAM_URL = process.env.STREAM_URL || "ws://localhost:3000/stream";
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;

if (!DEEPGRAM_API_KEY) {
  console.error(chalk.red("\n✖  Missing DEEPGRAM_API_KEY"));
  console.error(chalk.yellow("   1. Sign up free: https://console.deepgram.com/signup"));
  console.error(chalk.yellow("   2. Add DEEPGRAM_API_KEY=... to your .env"));
  console.error(chalk.yellow("   3. Run this script again.\n"));
  process.exit(1);
}

// Deepgram streaming endpoint, configured to match our exact audio format.
// encoding=linear16 → PCM16 LE | sample_rate=48000 | channels=1
const DEEPGRAM_URL =
  "wss://api.deepgram.com/v1/listen" +
  "?encoding=linear16&sample_rate=48000&channels=1" +
  "&punctuate=true&smart_format=true&interim_results=true";

console.log(chalk.bold.cyan("\nMeetStream Labs — Deepgram Reliability Test\n"));
console.log(chalk.dim(`Local stream : ${LOCAL_STREAM_URL}`));
console.log(chalk.dim(`Deepgram     : ${DEEPGRAM_URL.split("?")[0]}\n`));

// ── 1. Connect to Deepgram ───────────────────────────────────────────────────
const dgSocket = new WebSocket(DEEPGRAM_URL, {
  headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` },
});

let dgReady = false;
let localSocket = null;
let framesForwarded = 0;
let bytesForwarded = 0;

dgSocket.on("open", () => {
  dgReady = true;
  console.log(chalk.green("✔  Connected to Deepgram"));
  connectToLocalStream();

  // Keepalive — Deepgram closes the socket after ~10s of silence
  setInterval(() => {
    if (dgSocket.readyState === WebSocket.OPEN) {
      dgSocket.send(JSON.stringify({ type: "KeepAlive" }));
    }
  }, 8000);
});

dgSocket.on("message", (raw) => {
  let msg;
  try { msg = JSON.parse(raw.toString()); } catch { return; }

  if (msg.type === "Results") {
    const alt = msg.channel?.alternatives?.[0];
    if (!alt?.transcript) return;

    const isFinal = msg.is_final;
    const tag = isFinal ? chalk.green("✔ FINAL") : chalk.dim("… interim");
    const confidence = alt.confidence ? `[${alt.confidence.toFixed(2)}]` : "";

    if (alt.transcript.trim()) {
      console.log(`${tag}  ${chalk.white(alt.transcript)} ${chalk.dim(confidence)}`);
    }
  }

  if (msg.type === "Metadata") {
    console.log(chalk.dim(`  (Deepgram session: ${msg.request_id})`));
  }
});

dgSocket.on("error", (err) => {
  console.error(chalk.red("✖  Deepgram error:"), err.message);
});

dgSocket.on("close", (code, reason) => {
  console.log(chalk.yellow(`\nDeepgram connection closed (${code}) ${reason}`));
  console.log(chalk.dim(`  Total forwarded: ${(bytesForwarded/1024).toFixed(1)} KB across ${framesForwarded} frames`));
  process.exit(0);
});

// ── 2. Connect to YOUR local /stream and forward PCM to Deepgram ─────────────
function connectToLocalStream() {
  console.log(chalk.cyan(`Connecting to local stream: ${LOCAL_STREAM_URL} …`));
  localSocket = new WebSocket(LOCAL_STREAM_URL);

  localSocket.on("open", () => {
    console.log(chalk.green("✔  Connected to your /stream endpoint"));
    console.log(chalk.dim("Waiting for meeting audio… speak in the meeting now.\n"));
  });

  localSocket.on("message", (raw) => {
    const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);

    // Text frame = server handshake JSON ({ type: "ready", ... }) — ignore
    if (buf[0] === 0x7b) return;

    // Binary frame: [1B name_len][name][4B pcm_len LE][pcm]
    let offset = 0;
    const nameLen = buf.readUInt8(offset); offset += 1;
    offset += nameLen; // skip name — Deepgram only needs raw PCM
    const pcmLen = buf.readUInt32LE(offset); offset += 4;
    const pcm = buf.slice(offset, offset + pcmLen);

    if (pcm.length === 0) return;

    framesForwarded++;
    bytesForwarded += pcm.length;

    if (dgReady && dgSocket.readyState === WebSocket.OPEN) {
      dgSocket.send(pcm);
    }
  });

  localSocket.on("close", () => {
    console.log(chalk.yellow("Local stream closed."));
    if (dgSocket.readyState === WebSocket.OPEN) {
      dgSocket.send(JSON.stringify({ type: "CloseStream" }));
    }
  });

  localSocket.on("error", (err) => {
    console.error(chalk.red("✖  Local stream error:"), err.message);
    console.error(chalk.yellow("   Is `npm start` running in another terminal?"));
    process.exit(1);
  });
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
process.on("SIGINT", () => {
  console.log(chalk.dim("\nShutting down test…"));
  if (dgSocket.readyState === WebSocket.OPEN) {
    dgSocket.send(JSON.stringify({ type: "CloseStream" }));
    dgSocket.close();
  }
  if (localSocket) localSocket.close();
  setTimeout(() => process.exit(0), 500);
});