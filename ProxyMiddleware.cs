// ============================================================================
// ProxyMiddleware.cs — Core proxy logic
//
// This middleware handles every authenticated request:
//   1. Reads the client's request body into memory (needed for retries)
//   2. Acquires a key from the pool and builds an upstream request
//   3. Sends it to ElevenLabs and inspects the status code:
//      - 429 → cooldown this key, try the next one from the pool
//      - 500/503 → wait RETRY_DELAY_MS, retry up to RETRY_COUNT times
//      - 4xx → return to client immediately (client's fault, no retry)
//      - 2xx → stream the response body back to the client
//   4. Strips internal headers (concurrency counters, xi-api-key) from
//      the response so pool details are not leaked to clients.
//
// Streaming support:
//   - Response: chunked / audio / SSE responses are forwarded chunk-by-chunk
//     with immediate flush, so the client receives data in real-time.
//   - Request: the body is buffered once into memory to allow replaying on
//     retries (429 key rotation, 500/503 retries). This is fine for typical
//     ElevenLabs payloads (JSON text, usually < 100KB).
// ============================================================================

using System.Net;

namespace ElevenLabsProxy;

/// <summary>
/// ASP.NET Core middleware that transparently proxies requests to the
/// ElevenLabs API with automatic key rotation and error handling.
/// </summary>
public class ProxyMiddleware
{
    private readonly KeyPool _keyPool;
    private readonly ProxySettings _settings;
    private readonly HttpClient _httpClient;
    private readonly ILogger<ProxyMiddleware> _logger;

    /// <summary>
    /// HTTP hop-by-hop headers that must NOT be forwarded between client ↔ proxy ↔ upstream.
    /// See RFC 2616 §13.5.1.
    /// </summary>
    private static readonly HashSet<string> HopByHopHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
        "TE", "Trailers", "Transfer-Encoding", "Upgrade", "Host"
    };

    public ProxyMiddleware(
        RequestDelegate _,          // unused — this is a terminal middleware
        KeyPool keyPool,
        ProxySettings settings,
        IHttpClientFactory httpClientFactory,
        ILogger<ProxyMiddleware> logger)
    {
        _keyPool = keyPool;
        _settings = settings;
        _httpClient = httpClientFactory.CreateClient("ElevenLabs");
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var request = context.Request;
        var response = context.Response;

        // ── Buffer the request body ────────────────────────────────────
        // We need to replay it on retries, so read it fully into memory once.
        using var bodyStream = new MemoryStream();
        await request.Body.CopyToAsync(bodyStream);
        var bodyBytes = bodyStream.ToArray();

        // Track which keys have already been tried (for 429 rotation)
        var triedKeys = new HashSet<string>();
        HttpResponseMessage? lastUpstreamResponse = null;

        for (int attempt = 0; attempt < _settings.RetryCount; attempt++)
        {
            // ── Acquire a key from the pool ────────────────────────────
            var apiKey = _keyPool.Acquire(out var retryAfterMs);

            // All keys are on cooldown — tell the client to retry later
            if (apiKey == null)
            {
                response.StatusCode = 429;
                response.ContentType = "application/json";
                await response.WriteAsJsonAsync(new
                {
                    error = "all_keys_rate_limited",
                    message = "All API keys are currently rate-limited. Please retry later.",
                    retry_after_ms = retryAfterMs
                });
                return;
            }

            // ── Build the upstream request ─────────────────────────────
            var targetUrl = $"{_settings.ElevenLabsBaseUrl.TrimEnd('/')}{request.Path}{request.QueryString}";
            using var upstreamRequest = new HttpRequestMessage(new HttpMethod(request.Method), targetUrl);

            // Forward all client headers except hop-by-hop and the proxy's xi-api-key
            foreach (var header in request.Headers)
            {
                if (HopByHopHeaders.Contains(header.Key)) continue;
                if (header.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;

                // TryAddWithoutValidation returns false for content-specific headers
                // (e.g. Content-Type) — those are set on the content object below
                if (!upstreamRequest.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()))
                {
                    // Will be set on HttpContent below
                }
            }

            // Inject the real ElevenLabs API key from the pool
            upstreamRequest.Headers.Remove("xi-api-key");
            upstreamRequest.Headers.TryAddWithoutValidation("xi-api-key", apiKey);

            // Attach the request body for non-GET/HEAD methods
            if (request.Method != "GET" && request.Method != "HEAD" && bodyBytes.Length > 0)
            {
                var content = new ByteArrayContent(bodyBytes);

                if (request.ContentType != null)
                {
                    content.Headers.Remove("Content-Type");
                    content.Headers.TryAddWithoutValidation("Content-Type", request.ContentType);
                }

                upstreamRequest.Content = content;
            }

            // ── Send the request upstream ──────────────────────────────
            HttpResponseMessage upstreamResponse;
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_settings.RequestTimeoutSeconds));

                // ResponseHeadersRead: start processing as soon as headers arrive,
                // don't wait for the full body (important for streaming audio)
                upstreamResponse = await _httpClient.SendAsync(
                    upstreamRequest,
                    HttpCompletionOption.ResponseHeadersRead,
                    cts.Token);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Upstream request failed (attempt {Attempt}/{Max})", attempt + 1, _settings.RetryCount);

                // Out of retries — report a gateway error
                if (attempt + 1 >= _settings.RetryCount)
                {
                    response.StatusCode = 502;
                    response.ContentType = "application/json";
                    await response.WriteAsJsonAsync(new
                    {
                        error = "proxy_error",
                        message = $"Failed to reach ElevenLabs: {ex.Message}"
                    });
                    return;
                }

                await Task.Delay(_settings.RetryDelayMs);
                continue;
            }

            lastUpstreamResponse = upstreamResponse;
            var statusCode = (int)upstreamResponse.StatusCode;

            // ── 429: rate limited → cooldown this key, rotate to next ──
            if (statusCode == 429)
            {
                // Parse the Retry-After header if present (value is in seconds)
                int? retryAfter = null;
                if (upstreamResponse.Headers.TryGetValues("Retry-After", out var raValues))
                {
                    if (double.TryParse(raValues.FirstOrDefault(), out var raSec))
                        retryAfter = (int)(raSec * 1000); // convert to ms
                }

                _keyPool.MarkRateLimited(apiKey, retryAfter, _settings.KeyCooldownMs);
                triedKeys.Add(apiKey);

                // Must drain the body before disposing to allow connection reuse
                await upstreamResponse.Content.ReadAsByteArrayAsync();
                upstreamResponse.Dispose();

                // Still have untried keys — immediately try the next one
                if (triedKeys.Count < _keyPool.Size)
                {
                    continue;
                }

                // All keys exhausted — tell the client
                response.StatusCode = 429;
                response.ContentType = "application/json";
                await response.WriteAsJsonAsync(new
                {
                    error = "all_keys_rate_limited",
                    message = "All API keys are rate-limited."
                });
                return;
            }

            // ── 4xx: client error → return immediately, no retry ───────
            // These are the client's fault (bad request, auth error, etc.)
            if (statusCode >= 400 && statusCode < 500)
            {
                await ForwardResponse(upstreamResponse, response);
                return;
            }

            // ── 500 / 503: server error → retry after delay ────────────
            if (statusCode is 500 or 503)
            {
                _logger.LogWarning("Upstream returned {Status}, attempt {Attempt}/{Max}",
                    statusCode, attempt + 1, _settings.RetryCount);

                // Drain body before dispose so the connection can be reused
                await upstreamResponse.Content.ReadAsByteArrayAsync();
                upstreamResponse.Dispose();

                // Out of retries — forward the last error status
                if (attempt + 1 >= _settings.RetryCount)
                {
                    response.StatusCode = statusCode;
                    response.ContentType = "application/json";
                    await response.WriteAsJsonAsync(new
                    {
                        error = "upstream_error",
                        message = $"ElevenLabs returned {statusCode} after {_settings.RetryCount} retries."
                    });
                    return;
                }

                await Task.Delay(_settings.RetryDelayMs);
                continue;
            }

            // ── 2xx / other success → stream back to client ────────────
            await ForwardResponse(upstreamResponse, response);
            return;
        }
    }

    /// <summary>
    /// Copy upstream response status, headers, and body to the client.
    /// The body is streamed chunk-by-chunk (not buffered) so:
    ///   - Large audio files don't consume excessive memory
    ///   - Chunked/SSE streaming responses (e.g. streaming TTS) are forwarded
    ///     in real-time — each chunk is flushed to the client immediately
    /// </summary>
    private static async Task ForwardResponse(HttpResponseMessage upstream, HttpResponse client)
    {
        client.StatusCode = (int)upstream.StatusCode;

        // ── Detect streaming response ──────────────────────────────────
        // ElevenLabs uses chunked transfer encoding for streaming TTS.
        // We check both Transfer-Encoding and Content-Type to identify streams.
        var isChunked = upstream.Headers.TransferEncodingChunked == true;
        var contentType = upstream.Content.Headers.ContentType?.MediaType ?? "";
        var isStreaming = isChunked
            || contentType.Contains("text/event-stream")      // SSE
            || contentType.Contains("application/octet-stream") // raw binary stream
            || contentType.Contains("audio/");                  // streaming audio

        // ── Disable response buffering for streaming ───────────────────
        // Tell Kestrel not to buffer the response body — flush immediately.
        if (isStreaming)
        {
            var bufferingFeature = client.HttpContext.Features
                .Get<Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();
        }

        // ── Copy response headers ──────────────────────────────────────
        foreach (var header in upstream.Headers)
        {
            if (HopByHopHeaders.Contains(header.Key)) continue;
            if (header.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;

            // Strip ElevenLabs concurrency headers — they reveal pool internals
            if (header.Key.Equals("current-concurrent-requests", StringComparison.OrdinalIgnoreCase)) continue;
            if (header.Key.Equals("maximum-concurrent-requests", StringComparison.OrdinalIgnoreCase)) continue;

            client.Headers[header.Key] = header.Value.ToArray();
        }

        // Copy content headers (e.g. Content-Type: audio/mpeg, Content-Length, etc.)
        foreach (var header in upstream.Content.Headers)
        {
            // Skip Transfer-Encoding — Kestrel manages its own chunked encoding
            if (header.Key.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase)) continue;
            // Skip Content-Length for chunked responses — the proxy streams
            // without knowing the total size, so a fixed Content-Length would be wrong
            if (isChunked && header.Key.Equals("Content-Length", StringComparison.OrdinalIgnoreCase)) continue;

            client.Headers[header.Key] = header.Value.ToArray();
        }

        // ── Stream the body ────────────────────────────────────────────
        await using var upstreamStream = await upstream.Content.ReadAsStreamAsync();

        if (isStreaming)
        {
            // Streaming mode: read in small chunks and flush each one immediately.
            // This ensures the client receives audio/SSE data in real-time
            // rather than waiting for the entire response to complete.
            var buffer = new byte[8192]; // 8KB chunks — good balance between
                                         // syscall overhead and latency
            int bytesRead;
            while ((bytesRead = await upstreamStream.ReadAsync(buffer)) > 0)
            {
                await client.Body.WriteAsync(buffer.AsMemory(0, bytesRead));
                await client.Body.FlushAsync(); // push each chunk to the wire immediately
            }
        }
        else
        {
            // Non-streaming: copy the entire body at once (more efficient for
            // small JSON responses where latency per-chunk doesn't matter)
            await upstreamStream.CopyToAsync(client.Body);
        }
    }
}
