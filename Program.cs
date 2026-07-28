using ElevenLabsProxy;

var builder = WebApplication.CreateBuilder(args);

// ── Load settings ──────────────────────────────────────────────────────
var settings = new ProxySettings
{
    ProxyApiKey = Environment.GetEnvironmentVariable("PROXY_API_KEY") ?? string.Empty,
    ElevenLabsBaseUrl = Environment.GetEnvironmentVariable("ELEVENLABS_BASE_URL") ?? "https://api.elevenlabs.io",
    RetryCount = int.TryParse(Environment.GetEnvironmentVariable("RETRY_COUNT"), out var rc) ? rc : 3,
    RetryDelayMs = int.TryParse(Environment.GetEnvironmentVariable("RETRY_DELAY_MS"), out var rd) ? rd : 1000,
    KeyCooldownMs = int.TryParse(Environment.GetEnvironmentVariable("KEY_COOLDOWN_MS"), out var kc) ? kc : 60_000,
    RequestTimeoutSeconds = int.TryParse(Environment.GetEnvironmentVariable("REQUEST_TIMEOUT_SECONDS"), out var rt) ? rt : 120,
};

// Load API keys from env (comma-separated) or from file (one per line)
var keysEnv = Environment.GetEnvironmentVariable("ELEVENLABS_API_KEYS");
var keysFile = Environment.GetEnvironmentVariable("ELEVENLABS_API_KEYS_FILE");

if (!string.IsNullOrWhiteSpace(keysEnv))
{
    settings.ElevenLabsApiKeys = keysEnv
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .ToList();
}
else if (!string.IsNullOrWhiteSpace(keysFile) && File.Exists(keysFile))
{
    settings.ElevenLabsApiKeys = File.ReadAllLines(keysFile)
        .Select(l => l.Trim())
        .Where(l => !string.IsNullOrEmpty(l) && !l.StartsWith('#'))
        .ToList();
}

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

var keyPool = new KeyPool(settings.ElevenLabsApiKeys);

Console.WriteLine($"[server] Loaded {keyPool.Size} ElevenLabs API key(s)");

// ── Services ───────────────────────────────────────────────────────────
builder.Services.AddSingleton(settings);
builder.Services.AddSingleton(keyPool);
builder.Services.AddHttpClient("ElevenLabs")
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        MaxConnectionsPerServer = 100,
        EnableMultipleHttp2Connections = true,
    });

var app = builder.Build();

// ── Health / readiness endpoints (no auth) ─────────────────────────────
app.MapGet("/health", (KeyPool pool) => Results.Ok(new
{
    status = "ok",
    keys = pool.GetStatus()
}));

app.MapGet("/ready", (KeyPool pool) =>
{
    var available = pool.AvailableCount;
    return available > 0
        ? Results.Ok(new { status = "ready", available_keys = available, total_keys = pool.Size })
        : Results.Json(
            new { status = "all_keys_cooling_down", available_keys = 0, total_keys = pool.Size },
            statusCode: 503);
});

// ── Proxy: everything else goes through auth + proxy middleware ─────────
app.UseWhen(
    context =>
    {
        var path = context.Request.Path.Value ?? "";
        return path is not "/health" and not "/ready";
    },
    proxyApp =>
    {
        // Authentication gate
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

        // Proxy to ElevenLabs
        proxyApp.UseMiddleware<ProxyMiddleware>();
    });

app.Run();
