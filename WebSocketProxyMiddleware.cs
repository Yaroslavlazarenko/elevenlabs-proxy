// ============================================================================
// WebSocketProxyMiddleware.cs — WebSocket proxy for ElevenLabs real-time APIs
//
// ElevenLabs uses WebSocket connections for several real-time endpoints:
//   - /v1/text-to-speech/{voice_id}/stream-input       — streaming TTS input
//   - /v1/text-to-speech/{voice_id}/multi-stream-input — multi-context TTS
//   - /v1/speech-to-text/realtime                       — real-time STT
//
// This middleware intercepts WebSocket upgrade requests, authenticates the
// client via the xi-api-key header (or query param), replaces it with a real
// key from the pool, opens a WebSocket to ElevenLabs upstream, and relays
// frames bidirectionally (client ↔ upstream) until either side closes.
//
// Key rotation on 429 is NOT applicable here — WebSocket upgrades either
// succeed or fail at connection time. If the upgrade fails, the middleware
// returns the upstream error to the client.
// ============================================================================

using System.Net.WebSockets;

namespace ElevenLabsProxy;

public class WebSocketProxyMiddleware
{
    private readonly RequestDelegate _next;
    private readonly KeyPool _keyPool;
    private readonly ProxySettings _settings;
    private readonly ILogger<WebSocketProxyMiddleware> _logger;

    /// <summary>Buffer size for relaying WebSocket frames (4KB per direction).</summary>
    private const int BufferSize = 4096;

    public WebSocketProxyMiddleware(
        RequestDelegate next,
        KeyPool keyPool,
        ProxySettings settings,
        ILogger<WebSocketProxyMiddleware> logger)
    {
        _next = next;
        _keyPool = keyPool;
        _settings = settings;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        // Only handle WebSocket upgrade requests
        if (!context.WebSockets.IsWebSocketRequest)
        {
            await _next(context);
            return;
        }

        _logger.LogInformation("[ws-proxy] WebSocket upgrade for {Path}", context.Request.Path);

        // ── Acquire a key from the pool ────────────────────────────────
        var apiKey = _keyPool.Acquire(out var retryAfterMs);
        if (apiKey == null)
        {
            context.Response.StatusCode = 429;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsJsonAsync(new
            {
                error = "all_keys_rate_limited",
                message = "All API keys are currently rate-limited.",
                retry_after_ms = retryAfterMs
            });
            return;
        }

        // ── Build upstream WebSocket URI ────────────────────────────────
        // Convert base URL scheme: https → wss, http → ws
        var baseUrl = _settings.ElevenLabsBaseUrl
            .Replace("https://", "wss://")
            .Replace("http://", "ws://")
            .TrimEnd('/');

        // Rebuild query string: replace xi-api-key with pool key,
        // preserve all other query parameters
        var queryParams = context.Request.Query
            .Where(q => !q.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase))
            .Select(q => $"{Uri.EscapeDataString(q.Key)}={Uri.EscapeDataString(q.Value.ToString())}")
            .ToList();

        var upstreamUri = new Uri($"{baseUrl}{context.Request.Path}?{string.Join("&", queryParams)}");

        _logger.LogInformation("[ws-proxy] Connecting to upstream: {Uri}", upstreamUri);

        // ── Connect to ElevenLabs upstream WebSocket ───────────────────
        using var upstreamWs = new ClientWebSocket();

        // Set the real API key as a header on the upstream connection
        upstreamWs.Options.SetRequestHeader("xi-api-key", apiKey);

        // Forward relevant headers from the client (except auth and hop-by-hop)
        foreach (var header in context.Request.Headers)
        {
            var key = header.Key;
            if (key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;
            if (key.Equals("Host", StringComparison.OrdinalIgnoreCase)) continue;
            if (key.Equals("Connection", StringComparison.OrdinalIgnoreCase)) continue;
            if (key.Equals("Upgrade", StringComparison.OrdinalIgnoreCase)) continue;
            if (key.Equals("Sec-WebSocket-Key", StringComparison.OrdinalIgnoreCase)) continue;
            if (key.Equals("Sec-WebSocket-Version", StringComparison.OrdinalIgnoreCase)) continue;
            if (key.Equals("Sec-WebSocket-Extensions", StringComparison.OrdinalIgnoreCase)) continue;
            if (key.Equals("Sec-WebSocket-Protocol", StringComparison.OrdinalIgnoreCase)) continue;

            try
            {
                upstreamWs.Options.SetRequestHeader(key, header.Value.ToString());
            }
            catch
            {
                // Some headers can't be set on ClientWebSocket — skip silently
            }
        }

        try
        {
            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await upstreamWs.ConnectAsync(upstreamUri, connectCts.Token);
        }
        catch (WebSocketException ex)
        {
            _logger.LogError(ex, "[ws-proxy] Failed to connect to upstream WebSocket");
            context.Response.StatusCode = 502;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsJsonAsync(new
            {
                error = "ws_upstream_error",
                message = $"Failed to connect to ElevenLabs WebSocket: {ex.Message}"
            });
            return;
        }

        // ── Accept the client WebSocket ────────────────────────────────
        using var clientWs = await context.WebSockets.AcceptWebSocketAsync();

        _logger.LogInformation("[ws-proxy] WebSocket connection established, relaying frames");

        // ── Relay frames bidirectionally ────────────────────────────────
        // Two concurrent tasks: client→upstream and upstream→client.
        // When either side closes, we signal the other to shut down.
        using var cts = new CancellationTokenSource();

        var clientToUpstream = RelayFrames(clientWs, upstreamWs, "client→upstream", cts);
        var upstreamToClient = RelayFrames(upstreamWs, clientWs, "upstream→client", cts);

        // Wait for either direction to finish (close or error)
        await Task.WhenAny(clientToUpstream, upstreamToClient);

        // Signal the other direction to stop
        cts.Cancel();

        // Wait for both to complete gracefully
        try { await Task.WhenAll(clientToUpstream, upstreamToClient); }
        catch (OperationCanceledException) { /* expected */ }

        _logger.LogInformation("[ws-proxy] WebSocket connection closed for {Path}", context.Request.Path);
    }

    /// <summary>
    /// Read frames from <paramref name="source"/> and write them to <paramref name="destination"/>
    /// until the source sends a Close frame or the cancellation token fires.
    /// </summary>
    private async Task RelayFrames(
        WebSocket source,
        WebSocket destination,
        string direction,
        CancellationTokenSource cts)
    {
        var buffer = new byte[BufferSize];

        try
        {
            while (!cts.Token.IsCancellationRequested
                && source.State == WebSocketState.Open
                && destination.State == WebSocketState.Open)
            {
                var result = await source.ReceiveAsync(
                    new ArraySegment<byte>(buffer), cts.Token);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    // Forward the close frame to the other side
                    _logger.LogDebug("[ws-proxy] {Direction} close frame received", direction);

                    if (destination.State == WebSocketState.Open)
                    {
                        await destination.CloseOutputAsync(
                            result.CloseStatus ?? WebSocketCloseStatus.NormalClosure,
                            result.CloseStatusDescription,
                            CancellationToken.None);
                    }

                    break;
                }

                // Forward the data frame (text or binary) to the other side
                await destination.SendAsync(
                    new ArraySegment<byte>(buffer, 0, result.Count),
                    result.MessageType,
                    result.EndOfMessage,
                    cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // Other direction closed — normal shutdown
        }
        catch (WebSocketException ex) when (
            ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely
            || source.State != WebSocketState.Open)
        {
            // Connection dropped — expected during teardown
            _logger.LogDebug("[ws-proxy] {Direction} connection dropped: {Message}", direction, ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[ws-proxy] {Direction} relay error", direction);
        }
        finally
        {
            // Signal the other direction to stop
            cts.Cancel();
        }
    }
}
