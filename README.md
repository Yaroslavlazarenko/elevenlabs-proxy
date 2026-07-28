# ElevenLabs Proxy

A lightweight reverse proxy for the [ElevenLabs API](https://elevenlabs.io/docs/api-reference) that rotates through a pool of API keys, automatically handles rate limiting (429), and retries server errors (500/503).

Clients use the proxy exactly like they would use the ElevenLabs API directly — just point the base URL to the proxy and use the proxy's own authentication key.

## How It Works

```
┌────────┐   xi-api-key: PROXY_KEY   ┌───────────┐   xi-api-key: REAL_KEY_N   ┌────────────┐
│ Client │ ──────────────────────────▶│   Proxy   │ ─────────────────────────▶ │ ElevenLabs │
│        │ ◀──────────────────────────│           │ ◀───────────────────────── │    API     │
└────────┘    audio/json/websocket    └───────────┘    audio/json/websocket    └────────────┘
                                           │
                                      Key Pool (round-robin)
                                      ┌─────┬─────┬─────┐
                                      │ K1  │ K2  │ K3  │
                                      │ OK  │ 429 │ OK  │
                                      └─────┴─────┴─────┘
```

1. Client sends a request with `xi-api-key: <PROXY_API_KEY>` header
2. Proxy authenticates the client against `PROXY_API_KEY`
3. Proxy picks the next available key from the pool (round-robin)
4. Request is forwarded to ElevenLabs with the real API key
5. Response is streamed back to the client

### Streaming Support

The proxy fully supports all ElevenLabs streaming modes:

| Mode | Endpoints | How it works |
|---|---|---|
| **HTTP chunked** | `POST /v1/text-to-speech/{id}/stream`, `POST /v1/speech-to-speech/{id}/stream` | Audio chunks are flushed to the client immediately as they arrive from upstream — no buffering. |
| **WebSocket** | `GET /v1/text-to-speech/{id}/stream-input` | Bidirectional frame relay. Client sends text chunks, receives audio chunks in real-time. |
| **WebSocket multi-context** | `GET /v1/text-to-speech/{id}/multi-stream-input` | Same relay, multiple independent audio streams over one connection. |
| **WebSocket STT** | `GET /v1/speech-to-text/realtime` | Real-time speech-to-text. Audio frames in, transcription frames out. |

### Error Handling

| Upstream Status | Action |
|---|---|
| **429** (rate limited) | Key goes on cooldown, next key is tried immediately. If all keys are exhausted, returns 429 to client. |
| **500 / 503** (server error) | Retries after `RETRY_DELAY_MS` (default 1s), up to `RETRY_COUNT` times (default 3). |
| **400 / 401 / 4xx** (client error) | Returned to client immediately — no retry. |
| **2xx** (success) | Response body is streamed back to the client. |

When a key is rate-limited, the proxy respects the `Retry-After` header from ElevenLabs if present; otherwise it falls back to `KEY_COOLDOWN_MS` (default 60s).

## Quick Start

### 1. Clone and configure

```bash
git clone <repo-url> && cd elevenlabs-proxy
cp .env.example .env
```

Edit `.env`:
```env
PROXY_API_KEY=your-secret-proxy-key
```

### 2. Add your ElevenLabs API keys

Edit `keys.txt` (one key per line, `#` comments supported):
```
# Production keys
sk_aaaaaaaaaaaaaaaaaaaaaaaaaaaa
sk_bbbbbbbbbbbbbbbbbbbbbbbbbbbb
sk_cccccccccccccccccccccccccccc
```

### 3. Start

```bash
docker compose up -d --build
```

The proxy will be available at `http://localhost:3000`.

## Client Configuration

Point your ElevenLabs SDK or HTTP client to the proxy instead of the official API:

### Python (ElevenLabs SDK)

```python
from elevenlabs.client import ElevenLabs

client = ElevenLabs(
    api_key="your-secret-proxy-key",       # PROXY_API_KEY, not a real ElevenLabs key
    base_url="http://localhost:3000",       # proxy address
)

# Standard TTS
audio = client.text_to_speech.convert(
    voice_id="JBFqnCBsd6RMkjVDRZzb",
    text="Hello from the proxy!",
    model_id="eleven_flash_v2_5",
)

# Streaming TTS
audio_stream = client.text_to_speech.stream(
    voice_id="JBFqnCBsd6RMkjVDRZzb",
    text="This audio is streamed chunk by chunk.",
    model_id="eleven_flash_v2_5",
)
for chunk in audio_stream:
    process(chunk)
```

### JavaScript/TypeScript (ElevenLabs SDK)

```typescript
import { ElevenLabsClient } from "elevenlabs";

const client = new ElevenLabsClient({
    apiKey: "your-secret-proxy-key",
    baseUrl: "http://localhost:3000",
});

// Streaming TTS
const stream = await client.textToSpeech.stream("JBFqnCBsd6RMkjVDRZzb", {
    text: "Streamed through the proxy!",
    modelId: "eleven_flash_v2_5",
});
```

### cURL

```bash
# Standard TTS
curl -X POST http://localhost:3000/v1/text-to-speech/JBFqnCBsd6RMkjVDRZzb \
  -H "xi-api-key: your-secret-proxy-key" \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello!", "model_id": "eleven_flash_v2_5"}' \
  --output speech.mp3

# Streaming TTS
curl -X POST http://localhost:3000/v1/text-to-speech/JBFqnCBsd6RMkjVDRZzb/stream \
  -H "xi-api-key: your-secret-proxy-key" \
  -H "Content-Type: application/json" \
  -d '{"text": "Streamed!", "model_id": "eleven_flash_v2_5"}' \
  --output speech_stream.mp3
```

### WebSocket (Python)

```python
import asyncio, websockets, json

async def realtime_tts():
    uri = "ws://localhost:3000/v1/text-to-speech/JBFqnCBsd6RMkjVDRZzb/stream-input?model_id=eleven_flash_v2_5"
    headers = {"xi-api-key": "your-secret-proxy-key"}

    async with websockets.connect(uri, additional_headers=headers) as ws:
        # Init
        await ws.send(json.dumps({
            "text": " ",
            "voice_settings": {"stability": 0.5, "similarity_boost": 0.8},
        }))
        # Send text
        await ws.send(json.dumps({"text": "Hello from WebSocket!"}))
        # Close signal
        await ws.send(json.dumps({"text": ""}))
        # Receive audio chunks
        async for msg in ws:
            data = json.loads(msg)
            if data.get("audio"):
                process_audio(data["audio"])
            if data.get("isFinal"):
                break

asyncio.run(realtime_tts())
```

## Configuration

All settings are configured via environment variables (in `.env`):

| Variable | Default | Description |
|---|---|---|
| `PROXY_API_KEY` | *(required)* | Secret key clients use to authenticate with the proxy |
| `ELEVENLABS_API_KEYS` | — | Comma-separated list of ElevenLabs API keys |
| `ELEVENLABS_API_KEYS_FILE` | — | Path to a file with one key per line (alternative to above) |
| `ELEVENLABS_BASE_URL` | `https://api.elevenlabs.io` | Upstream API base URL |
| `PORT` | `3000` | Port the proxy listens on |
| `RETRY_COUNT` | `3` | Max retry attempts for 500/503 errors |
| `RETRY_DELAY_MS` | `1000` | Delay between retries (ms) |
| `KEY_COOLDOWN_MS` | `60000` | Default cooldown for rate-limited keys (ms) |
| `REQUEST_TIMEOUT_SECONDS` | `120` | Upstream request timeout |

> **Note:** `ELEVENLABS_API_KEYS` takes priority over `ELEVENLABS_API_KEYS_FILE`. You only need one of them.

## API Endpoints

### Proxy (all ElevenLabs routes)

All requests except `/health` and `/ready` are proxied to ElevenLabs. Both HTTP and WebSocket requests are supported. Requires `xi-api-key` header with the proxy key.

### Health Check

```
GET /health
```

Returns status of all keys in the pool (no authentication required):

```json
{
  "status": "ok",
  "keys": [
    { "index": 0, "keySuffix": "...b842d9", "available": true, "cooldownRemainingMs": 0 },
    { "index": 1, "keySuffix": "...a1b2c3", "available": false, "cooldownRemainingMs": 45200 }
  ]
}
```

### Readiness Probe

```
GET /ready
```

Returns `200` if at least one key is available, `503` if all keys are on cooldown. Suitable for Kubernetes/Docker health checks.

## Project Structure

```
elevenlabs-proxy/
├── Program.cs                    # Entry point, config loading, middleware pipeline
├── ProxySettings.cs              # Configuration model
├── KeyPool.cs                    # Thread-safe round-robin key pool with cooldown
├── ProxyMiddleware.cs            # HTTP proxy (forwarding, retries, streaming responses)
├── WebSocketProxyMiddleware.cs   # WebSocket proxy (bidirectional frame relay)
├── ElevenLabsProxy.csproj        # .NET 8 project file
├── Dockerfile                    # Multi-stage build (SDK -> Alpine runtime)
├── docker-compose.yml            # Container orchestration
├── keys.txt                      # ElevenLabs API keys (one per line)
├── .env.example                  # Configuration template
├── .dockerignore
└── .gitignore
```

## Tech Stack

- **Runtime:** .NET 8 / ASP.NET Core (minimal API)
- **Container:** Alpine-based Docker image (~100MB)
- **Dependencies:** None beyond the .NET standard library

## License

MIT
