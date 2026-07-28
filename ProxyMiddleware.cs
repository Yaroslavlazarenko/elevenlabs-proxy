using System.Net;

namespace ElevenLabsProxy;

/// <summary>
/// Middleware that proxies all requests (except /health, /ready) to ElevenLabs
/// with key rotation, 429 cooldown, and 500/503 retries.
/// </summary>
public class ProxyMiddleware
{
    private readonly KeyPool _keyPool;
    private readonly ProxySettings _settings;
    private readonly HttpClient _httpClient;
    private readonly ILogger<ProxyMiddleware> _logger;

    private static readonly HashSet<string> HopByHopHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
        "TE", "Trailers", "Transfer-Encoding", "Upgrade", "Host"
    };

    public ProxyMiddleware(
        RequestDelegate _,
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

        // Read the incoming body once
        using var bodyStream = new MemoryStream();
        await request.Body.CopyToAsync(bodyStream);
        var bodyBytes = bodyStream.ToArray();

        var triedKeys = new HashSet<string>();
        HttpResponseMessage? lastUpstreamResponse = null;

        for (int attempt = 0; attempt < _settings.RetryCount; attempt++)
        {
            // Acquire key
            var apiKey = _keyPool.Acquire(out var retryAfterMs);

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

            // Build upstream request
            var targetUrl = $"{_settings.ElevenLabsBaseUrl.TrimEnd('/')}{request.Path}{request.QueryString}";

            using var upstreamRequest = new HttpRequestMessage(new HttpMethod(request.Method), targetUrl);

            // Forward headers
            foreach (var header in request.Headers)
            {
                if (HopByHopHeaders.Contains(header.Key)) continue;
                if (header.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;

                // Content headers go on Content, others on Request
                if (!upstreamRequest.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()))
                {
                    // Will be set on content below
                }
            }

            // Set the real ElevenLabs key
            upstreamRequest.Headers.Remove("xi-api-key");
            upstreamRequest.Headers.TryAddWithoutValidation("xi-api-key", apiKey);

            // Set body
            if (request.Method != "GET" && request.Method != "HEAD" && bodyBytes.Length > 0)
            {
                var content = new ByteArrayContent(bodyBytes);

                // Copy content-type
                if (request.ContentType != null)
                {
                    content.Headers.Remove("Content-Type");
                    content.Headers.TryAddWithoutValidation("Content-Type", request.ContentType);
                }

                upstreamRequest.Content = content;
            }

            HttpResponseMessage upstreamResponse;
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_settings.RequestTimeoutSeconds));
                upstreamResponse = await _httpClient.SendAsync(
                    upstreamRequest,
                    HttpCompletionOption.ResponseHeadersRead,
                    cts.Token);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Upstream request failed (attempt {Attempt}/{Max})", attempt + 1, _settings.RetryCount);

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

            // ── 429: rate limited → cooldown key, try next ─────────────────
            if (statusCode == 429)
            {
                int? retryAfter = null;
                if (upstreamResponse.Headers.TryGetValues("Retry-After", out var raValues))
                {
                    if (double.TryParse(raValues.FirstOrDefault(), out var raSec))
                        retryAfter = (int)(raSec * 1000);
                }

                _keyPool.MarkRateLimited(apiKey, retryAfter, _settings.KeyCooldownMs);
                triedKeys.Add(apiKey);

                // Drain body
                await upstreamResponse.Content.ReadAsByteArrayAsync();
                upstreamResponse.Dispose();

                if (triedKeys.Count < _keyPool.Size)
                {
                    continue; // try next key immediately
                }

                // All keys exhausted
                response.StatusCode = 429;
                response.ContentType = "application/json";
                await response.WriteAsJsonAsync(new
                {
                    error = "all_keys_rate_limited",
                    message = "All API keys are rate-limited."
                });
                return;
            }

            // ── 4xx (400, 401, etc.): client error → return immediately ────
            if (statusCode >= 400 && statusCode < 500)
            {
                await ForwardResponse(upstreamResponse, response);
                return;
            }

            // ── 500 / 503: server error → retry after delay ────────────────
            if (statusCode is 500 or 503)
            {
                _logger.LogWarning("Upstream returned {Status}, attempt {Attempt}/{Max}",
                    statusCode, attempt + 1, _settings.RetryCount);

                await upstreamResponse.Content.ReadAsByteArrayAsync();
                upstreamResponse.Dispose();

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

            // ── 2xx / other success → forward to client ────────────────────
            await ForwardResponse(upstreamResponse, response);
            return;
        }
    }

    private static async Task ForwardResponse(HttpResponseMessage upstream, HttpResponse client)
    {
        client.StatusCode = (int)upstream.StatusCode;

        // Copy response headers
        foreach (var header in upstream.Headers)
        {
            if (HopByHopHeaders.Contains(header.Key)) continue;
            if (header.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;
            // Strip concurrency headers that reveal pool internals
            if (header.Key.Equals("current-concurrent-requests", StringComparison.OrdinalIgnoreCase)) continue;
            if (header.Key.Equals("maximum-concurrent-requests", StringComparison.OrdinalIgnoreCase)) continue;

            client.Headers[header.Key] = header.Value.ToArray();
        }

        // Copy content headers
        foreach (var header in upstream.Content.Headers)
        {
            if (header.Key.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase)) continue;
            client.Headers[header.Key] = header.Value.ToArray();
        }

        // Stream the body
        await using var upstreamStream = await upstream.Content.ReadAsStreamAsync();
        await upstreamStream.CopyToAsync(client.Body);
    }
}
