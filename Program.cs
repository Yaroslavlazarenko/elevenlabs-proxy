// ============================================================================
// Program.cs — Application entry point
//
// Bootstraps the ASP.NET Core pipeline:
//   1. Reads configuration from environment variables
//   2. Loads ElevenLabs API keys into a round-robin KeyPool
//   3. Registers /health and /ready endpoints (no auth required)
//   4. Wires up authentication + proxy middleware for everything else
// ============================================================================

using ElevenLabsProxy;

var builder = WebApplication.CreateBuilder(args);

// ── Load settings from environment variables ───────────────────────────
// Every setting has a sensible default; only PROXY_API_KEY and at least
// one ElevenLabs key are mandatory.
var settings = new ProxySettings
{
    ProxyApiKey = Environment.GetEnvironmentVariable("PROXY_API_KEY") ?? string.Empty,
    ElevenLabsBaseUrl = Environment.GetEnvironmentVariable("ELEVENLABS_BASE_URL") ?? "https://api.elevenlabs.io",
    RetryCount = int.TryParse(Environment.GetEnvironmentVariable("RETRY_COUNT"), out var rc) ? rc : 3,
    RetryDelayMs = int.TryParse(Environment.GetEnvironmentVariable("RETRY_DELAY_MS"), out var rd) ? rd : 1000,
    KeyCooldownMs = int.TryParse(Environment.GetEnvironmentVariable("KEY_COOLDOWN_MS"), out var kc) ? kc : 60_000,
    RequestTimeoutSeconds = int.TryParse(Environment.GetEnvironmentVariable("REQUEST_TIMEOUT_SECONDS"), out var rt) ? rt : 120,
};

// ── Load ElevenLabs API keys ───────────────────────────────────────────
// Two sources are supported (first match wins):
//   ELEVENLABS_API_KEYS      — comma-separated inline list
//   ELEVENLABS_API_KEYS_FILE — path to a text file, one key per line
var keysEnv = Environment.GetEnvironmentVariable("ELEVENLABS_API_KEYS");
var keysFile = Environment.GetEnvironmentVariable("ELEVENLABS_API_KEYS_FILE");

if (!string.IsNullOrWhiteSpace(keysEnv))
{
    // Inline comma-separated keys (e.g. "sk-aaa,sk-bbb,sk-ccc")
    settings.ElevenLabsApiKeys = keysEnv
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .ToList();
}
else if (!string.IsNullOrWhiteSpace(keysFile) && File.Exists(keysFile))
{
    // File-based keys: blank lines and lines starting with '#' are ignored
    settings.ElevenLabsApiKeys = File.ReadAllLines(keysFile)
        .Select(l => l.Trim())
        .Where(l => !string.IsNullOrEmpty(l) && !l.StartsWith('#'))
        .ToList();
}

// ── Validate required configuration ────────────────────────────────────
if (settings.ElevenLabsApiKeys.Count == 0)
{
    Console.Error.WriteLine("[server] No ElevenLabs API keys configured.");
    Console.Error.WriteLine("Set ELEVENLABS_API_KEYS (comma-separated) or ELEVENLABS_API_KEYS_FILE.");
    Environment.Exit(1);
}

if (string.IsNullOrWhiteSpace(settings.ProxyApiKey))
{
    Console.Error.WriteLine("[server] PROXY_API_KEY is not set. Clients must authenticate with this key.");
    Environment.Exit(1);
}

// ── Initialize key pool ────────────────────────────────────────────────
var keyPool = new KeyPool(settings.ElevenLabsApiKeys);
Console.WriteLine($"[server] Loaded {keyPool.Size} ElevenLabs API key(s)");

// ── Register services in DI container ──────────────────────────────────
builder.Services.AddSingleton(settings);
builder.Services.AddSingleton(keyPool);

// Named HttpClient with connection pooling tuned for high-throughput proxying
builder.Services.AddHttpClient("ElevenLabs")
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),  // recycle connections periodically
        MaxConnectionsPerServer = 100,                       // allow many parallel upstream calls
        EnableMultipleHttp2Connections = true,
    });

var app = builder.Build();

// ── Health / readiness endpoints (no authentication required) ──────────
// GET /health — returns status of every key in the pool (available or cooling down)
app.MapGet("/health", (KeyPool pool) => Results.Ok(new
{
    status = "ok",
    keys = pool.GetStatus()
}));

// GET /ready — returns 200 if at least one key is available, 503 otherwise.
// Used as a Kubernetes/Docker readiness probe.
app.MapGet("/ready", (KeyPool pool) =>
{
    var available = pool.AvailableCount;
    return available > 0
        ? Results.Ok(new { status = "ready", available_keys = available, total_keys = pool.Size })
        : Results.Json(
            new { status = "all_keys_cooling_down", available_keys = 0, total_keys = pool.Size },
            statusCode: 503);
});

// ── Proxy pipeline (auth + forwarding) ─────────────────────────────────
// UseWhen branches the middleware pipeline: only requests that are NOT
// /health or /ready go through authentication and proxying.
app.UseWhen(
    context =>
    {
        var path = context.Request.Path.Value ?? "";
        return path is not "/health" and not "/ready";
    },
    proxyApp =>
    {
        // Authentication gate: compare the client's xi-api-key header
        // against the configured PROXY_API_KEY. Reject with 401 on mismatch.
        proxyApp.Use(async (context, next) =>
        {
            var clientKey = context.Request.Headers["xi-api-key"].FirstOrDefault();
            if (string.IsNullOrEmpty(clientKey) || clientKey != settings.ProxyApiKey)
            {
                context.Response.StatusCode = 401;
                context.Response.ContentType = "application/json";
                await context.Response.WriteAsJsonAsync(new
                {
                    error = "unauthorized",
                    message = "Invalid or missing xi-api-key header."
                });
                return;
            }

            await next();
        });

        // All authenticated requests are forwarded to ElevenLabs via ProxyMiddleware
        proxyApp.UseMiddleware<ProxyMiddleware>();
    });

app.Run();
