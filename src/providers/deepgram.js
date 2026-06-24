/**
 * Deepgram provider
 * ─────────────────────────────────────────────────────────────────────────────
 * Free tier: https://console.deepgram.com/signup ($200 credit, no card)
 * Env required: DEEPGRAM_API_KEY
 *
 * Streams raw PCM straight through to Deepgram's real-time STT endpoint and
 * surfaces interim + final transcripts via onResult().
 */

import { WebSocket } from "ws";

const DEEPGRAM_URL =
  "wss://api.deepgram.com/v1/listen" +
  "?encoding=linear16&sample_rate=48000&channels=1" +
  "&punctuate=true&smart_format=true&interim_results=true";

export default {
  name: "Deepgram (speech-to-text)",

  async connect(onResult) {
    const apiKey = process.env.DEEPGRAM_API_KEY;
    if (!apiKey) {
      throw new Error(
        "Missing DEEPGRAM_API_KEY. Get a free key: https://console.deepgram.com/signup"
      );
    }

    return new Promise((resolve, reject) => {
      this.socket = new WebSocket(DEEPGRAM_URL, {
        headers: { Authorization: `Token ${apiKey}` },
      });

      this.socket.on("open", () => {
        // Keepalive every 8s so Deepgram doesn't close on silence
        this.keepAliveTimer = setInterval(() => {
          if (this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify({ type: "KeepAlive" }));
          }
        }, 8000);
        resolve();
      });

      this.socket.on("message", (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch { return; }

        if (msg.type === "Results") {
          const alt = msg.channel?.alternatives?.[0];
          if (alt?.transcript?.trim()) {
            onResult(alt.transcript, msg.is_final, { confidence: alt.confidence });
          }
        }
      });

      this.socket.on("error", (err) => reject(err));
    });
  },

  sendAudio(pcm) {
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(pcm);
    }
  },

  async disconnect() {
    clearInterval(this.keepAliveTimer);
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify({ type: "CloseStream" }));
      this.socket.close();
    }
  },
};
