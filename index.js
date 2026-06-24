#!/usr/bin/env node
/**
 * MeetStream Labs — Real-Time Audio Streaming Example
 * ====================================================
 * Clone → fill .env → node index.js
 *
 * What this does:
 *  1. Spins up an ngrok HTTPS tunnel (no public server needed)
 *  2. Starts a local Express server with these endpoints:
 *       POST /webhook/callback    — bot lifecycle events (MeetStream → us)
 *       POST /webhook/transcript  — live transcript segments (MeetStream → us)
 *       WS   /audio              — raw PCM intake from MeetStream bot
 *       WS   /stream             — live PCM broadcast TO external consumers ◄ NEW
 *       GET  /health             — status check
 *  3. Calls MeetStream API to deploy a bot into your meeting
 *  4. Re-broadcasts every audio frame to any client on WS /stream in real time
 *  5. Saves per-speaker .wav files to ./logs/audio/ as an archive
 */

import "dotenv/config";
import ngrok from "@ngrok/ngrok";
import express from "express";
import expressWs from "express-ws";
import chalk from "chalk";
import { createServer } from "http";
import { MeetStreamClient } from "./src/meetstream.js";
import { AudioHandler } from "./src/audio.js";
import { Broadcaster } from "./src/broadcaster.js";
import { Logger } from "./src/logger.js";

// ── Config validation ─────────────────────────────────────────────────────────
const REQUIRED = ["MEETSTREAM_API_KEY", "NGROK_AUTHTOKEN", "MEETING_LINK"];
for (const key of REQUIRED) {
  if (!process.env[key]) {
    console.error(chalk.red(`\n✖  Missing required env var: ${key}`));
    console.error(chalk.yellow(`   Copy .env.example → .env and fill it in.\n`));
    process.exit(1);
  }
}

const PORT = parseInt(process.env.PORT || "3000", 10);
const logger      = new Logger();
const broadcaster = new Broadcaster(logger);
const audioHandler = new AudioHandler(logger, broadcaster);

// ── Express + WebSocket server ────────────────────────────────────────────────
const app = express();
const server = createServer(app);
expressWs(app, server);

app.use(express.json());

let botId = null;

// ── Routes ────────────────────────────────────────────────────────────────────

/**
 * POST /webhook/callback
 * Bot lifecycle events: joining → InMeeting → Stopped, participant join/leave, etc.
 */
app.post("/webhook/callback", (req, res) => {
  res.sendStatus(200);
  logger.event(req.body);
});

/**
 * POST /webhook/transcript
 * Live transcript segments pushed by MeetStream in real time.
 */
app.post("/webhook/transcript", (req, res) => {
  res.sendStatus(200);
  const { speakerName, transcript, timestamp, words } = req.body;
  if (transcript?.trim()) {
    logger.transcript(speakerName, transcript, timestamp, words);
  }
});

/**
 * WS /audio  (INTAKE — MeetStream connects here)
 * Receives binary PCM frames from the bot.
 * Each frame is parsed by AudioHandler which:
 *   • saves it to a per-speaker .wav file
 *   • forwards it to Broadcaster → all /stream clients
 */
app.ws("/audio", (ws) => {
  logger.info(chalk.cyan("MeetStream audio WebSocket connected"));

  ws.on("message", (data) => {
    if (Buffer.isBuffer(data)) {
      audioHandler.handleFrame(data);
    } else {
      try { audioHandler.handleMeta(JSON.parse(data)); } catch { /* ignore */ }
    }
  });

  ws.on("close", () => {
    logger.info(chalk.yellow("MeetStream audio WebSocket closed"));
    audioHandler.flush();
  });

  ws.on("error", (err) => logger.error("MeetStream audio WS error", err));
});

/**
 * WS /stream  (OUTPUT — external apps connect here)
 * ─────────────────────────────────────────────────
 * Any external application connects here and receives live audio
 * frames as they arrive from the meeting, with zero added latency.
 *
 * Frame format sent to clients:
 *   1 byte   name_length
 *   N bytes  speaker_name (UTF-8)
 *   4 bytes  pcm_length   (uint32 LE)
 *   M bytes  pcm_data     (PCM16 LE, 48kHz, mono)
 *
 * On connect: client receives a JSON "ready" message with format info.
 * Multiple clients can connect simultaneously — all get the same stream.
 *
 * Example client (Node.js):
 *   const ws = new WebSocket("ws://localhost:3000/stream");
 *   ws.on("message", (buf) => { ... parse frame ... });
 */
app.ws("/stream", (ws) => {
  broadcaster.add(ws);
});

/**
 * GET /health
 */
app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    botId,
    streamClients: broadcaster.size,
  });
});

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  logger.banner();

  // 1. Start local HTTP/WS server
  await new Promise((resolve) => server.listen(PORT, resolve));
  logger.info(`Local server on port ${chalk.green(PORT)}`);
  logger.info(`  Intake  : ws://localhost:${PORT}/audio   (MeetStream → here)`);
  logger.info(`  Stream  : ws://localhost:${PORT}/stream  (here → external apps)`);

  // 2. Open ngrok tunnel
  logger.info("Opening ngrok tunnel…");
  const listener = await ngrok.connect({
    addr: PORT,
    authtoken: process.env.NGROK_AUTHTOKEN,
  });
  const publicUrl = listener.url();
  logger.info(`ngrok tunnel: ${chalk.green(publicUrl)}`);

  // 3. Build URLs
  const callbackUrl    = `${publicUrl}/webhook/callback`;
  const transcriptUrl  = `${publicUrl}/webhook/transcript`;
  const audioWsUrl     = `${publicUrl.replace("https://", "wss://")}/audio`;

  logger.info(`Callback URL  : ${chalk.cyan(callbackUrl)}`);
  logger.info(`Transcript URL: ${chalk.cyan(transcriptUrl)}`);
  logger.info(`Audio WS URL  : ${chalk.cyan(audioWsUrl)}`);

  // 4. Create the MeetStream bot
  const client = new MeetStreamClient(process.env.MEETSTREAM_API_KEY, logger);
  botId = await client.createBot({
    meetingLink: process.env.MEETING_LINK,
    callbackUrl,
    transcriptWebhookUrl: transcriptUrl,
    audioWsUrl,
  });

  logger.success(`Bot created! ID: ${chalk.bold(botId)}`);
  logger.info("Waiting for bot to join the meeting…");
  logger.info(chalk.dim("Press Ctrl+C to stop the bot and exit.\n"));

  // 5. Graceful shutdown
  for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, async () => {
      console.log("");
      logger.info("Shutting down…");
      if (botId) {
        await client.removeBot(botId).catch(() => {});
        logger.info(`Bot ${botId} removed.`);
      }
      audioHandler.flush();
      await ngrok.disconnect().catch(() => {});
      server.close();
      process.exit(0);
    });
  }
}

main().catch((err) => {
  console.error(chalk.red("\n✖  Fatal error:"), err.message);
  process.exit(1);
});