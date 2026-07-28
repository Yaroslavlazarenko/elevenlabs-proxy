// ============================================================================
// ProxyMiddleware.cs — Core proxy logic
//
// This middleware handles every authenticated request:
//   1. Reads the client's request body into memory (needed for retries)
//   2. Acquires a key from the pool (with concurrency slot)
//   3. Sends it to ElevenLabs and inspects the status code:
//      - 429 → cooldown this key, release slot, try next key
//      - 401 detected_unusual_activity → disable key permanently
//      - 401/402 quota_exceeded → send key to back, try next
//      - 500/503 → retry up to RetryCount per key, then next key
//      - 4xx → return to client immediately
//      - 2xx → stream response, then release slot
//   4. Release() is always called via try/finally to free concurrency slots
// ============================================================================

using System.Net;

namespace ElevenLabsProxy;

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

        // Buffer the request body once — needed for retries
        using var bodyStream = new MemoryStream();
        await request.Body.CopyToAsync(bodyStream);
        var bodyBytes = bodyStream.ToArray();

        var triedKeys = new HashSet<string>();
        var serverErrorsByKey = new Dictionary<string, int>();
        var maxAttempts = _keyPool.Size * (_settings.RetryCount + 1);
        string? lastRateLimitBody = null;
        string? lastErrorBody = null;
        int lastErrorStatus = 429;

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            // ── Acquire a key (with concurrency slot) ──────────────────
            var apiKey = _keyPool.Acquire(out var retryAfterMs);

            if (apiKey == null)
            {
                // No key available — return last real error or generic 429
                if (lastRateLimitBody != null)
                {
                    response.StatusCode = 429;
                    response.ContentType = "application/json";
                    await response.WriteAsync(lastRateLimitBody);
                }
                else if (lastErrorBody != null)
                {
                    response.StatusCode = lastErrorStatus;
                    response.ContentType = "application/json";
                    await response.WriteAsync(lastErrorBody);
                }
                else
                {
                    response.StatusCode = 429;
                    response.ContentType = "application/json";
                    await response.WriteAsJsonAsync(new
                    {
                        detail = new
                        {
                            type = "rate_limit_error",
                            code = "rate_limit_exceeded",
                            message = "Too many requests. Please retry later.",
                            status = "rate_limit_exceeded"
                        }
                    });
                }
                return;
            }

            try
            {
                // ── Build upstream request ─────────────────────────────
                var targetUrl = $"{_settings.ElevenLabsBaseUrl.TrimEnd('/')}{request.Path}{request.QueryString}";
                using var upstreamRequest = new HttpRequestMessage(new HttpMethod(request.Method), targetUrl);

                foreach (var header in request.Headers)
                {
                    if (HopByHopHeaders.Contains(header.Key)) continue;
                    if (header.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;
                    upstreamRequest.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
                }

                upstreamRequest.Headers.Remove("xi-api-key");
                upstreamRequest.Headers.TryAddWithoutValidation("xi-api-key", apiKey);

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

                // ── Send upstream ──────────────────────────────────────
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
                    _keyPool.Release(apiKey);

                    serverErrorsByKey.TryGetValue(apiKey, out var errs);
                    errs++;
                    serverErrorsByKey[apiKey] = errs;

                    _logger.LogError(ex, "Upstream request failed, key ...{KeySuffix} attempt {Attempt}/{Max}",
                        apiKey[^6..], errs, _settings.RetryCount);

                    if (errs >= _settings.RetryCount)
                    {
                        triedKeys.Add(apiKey);
                        if (triedKeys.Count >= _keyPool.Size)
                        {
                            response.StatusCode = 502;
                            response.ContentType = "application/json";
                            await response.WriteAsJsonAsync(new
                            {
                                detail = new
                                {
                                    type = "internal_error",
                                    code = "internal_error",
                                    message = $"Failed to reach ElevenLabs: {ex.Message}",
                                    status = "internal_error"
                                }
                            });
                            return;
                        }
                    }

                    await Task.Delay(_settings.RetryDelayMs);
                    continue;
                }

                var statusCode = (int)upstreamResponse.StatusCode;

                // ── 429: rate limited ──────────────────────────────────
                if (statusCode == 429)
                {
                    int? retryAfter = null;
                    if (upstreamResponse.Headers.TryGetValues("Retry-After", out var raValues))
                    {
                        if (double.TryParse(raValues.FirstOrDefault(), out var raSec))
                            retryAfter = (int)(raSec * 1000);
                    }

                    lastRateLimitBody = await upstreamResponse.Content.ReadAsStringAsync();
                    upstreamResponse.Dispose();

                    _keyPool.Release(apiKey);
                    _keyPool.MarkRateLimited(apiKey, retryAfter, _settings.KeyCooldownMs);
                    triedKeys.Add(apiKey);

                    if (triedKeys.Count < _keyPool.Size) continue;

                    // All keys exhausted — forward last 429 from ElevenLabs
                    response.StatusCode = 429;
                    response.ContentType = "application/json";
                    await response.WriteAsync(lastRateLimitBody);
                    return;
                }

                // ── 401/402: check for quota or ban ───────────────────
                if (statusCode is 401 or 402)
                {
                    var errorBody = await upstreamResponse.Content.ReadAsStringAsync();
                    lastErrorBody = errorBody;
                    lastErrorStatus = statusCode;

                    // detected_unusual_activity → disable key permanently
                    if (errorBody.Contains("detected_unusual_activity", StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogWarning("Key ...{KeySuffix} BANNED (detected_unusual_activity), disabling",
                            apiKey[^6..]);

                        _keyPool.Release(apiKey);
                        _keyPool.Disable(apiKey);
                        triedKeys.Add(apiKey);

                        if (triedKeys.Count < _keyPool.Size) continue;

                        await ForwardResponseFromBytes(upstreamResponse, errorBody, response);
                        return;
                    }

                    // quota_exceeded → cooldown + send to back, try next key
                    if (errorBody.Contains("quota_exceeded", StringComparison.OrdinalIgnoreCase)
                        || errorBody.Contains("insufficient_credits", StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogWarning("Key ...{KeySuffix} quota exceeded, cooldown {Cooldown}ms, trying next key",
                            apiKey[^6..], _settings.QuotaCooldownMs);

                        _keyPool.Release(apiKey);
                        _keyPool.MarkRateLimited(apiKey, _settings.QuotaCooldownMs, _settings.QuotaCooldownMs);
                        _keyPool.SendToBack(apiKey);
                        triedKeys.Add(apiKey);
                        upstreamResponse.Dispose();

                        if (triedKeys.Count < _keyPool.Size) continue;

                        await ForwardResponseFromBytes(upstreamResponse, errorBody, response);
                        return;
                    }

                    // Real auth error — forward as-is
                    _keyPool.Release(apiKey);
                    await ForwardResponseFromBytes(upstreamResponse, errorBody, response);
                    return;
                }

                // ── Other 4xx: client error, no retry ─────────────────
                if (statusCode >= 400 && statusCode < 500)
                {
                    _keyPool.Release(apiKey);
                    await ForwardResponse(upstreamResponse, response);
                    return;
                }

                // ── 500/503: server error, retry per key ──────────────
                if (statusCode is 500 or 503)
                {
                    serverErrorsByKey.TryGetValue(apiKey, out var keyErrors);
                    keyErrors++;
                    serverErrorsByKey[apiKey] = keyErrors;

                    _logger.LogWarning("Upstream returned {Status}, key ...{KeySuffix} attempt {Attempt}/{Max}",
                        statusCode, apiKey[^6..], keyErrors, _settings.RetryCount);

                    lastErrorBody = await upstreamResponse.Content.ReadAsStringAsync();
                    lastErrorStatus = statusCode;
                    upstreamResponse.Dispose();

                    _keyPool.Release(apiKey);

                    if (keyErrors >= _settings.RetryCount)
                    {
                        triedKeys.Add(apiKey);
                        if (triedKeys.Count >= _keyPool.Size)
                        {
                            response.StatusCode = statusCode;
                            response.ContentType = "application/json";
                            await response.WriteAsync(lastErrorBody);
                            return;
                        }
                    }

                    await Task.Delay(_settings.RetryDelayMs);
                    continue;
                }

                // ── 2xx: success → stream to client, then release ─────
                await ForwardResponse(upstreamResponse, response);
                _keyPool.Release(apiKey);
                return;
            }
            catch
            {
                // Safety net: always release the concurrency slot
                _keyPool.Release(apiKey);
                throw;
            }
        }
    }

    /// <summary>
    /// Stream upstream response to client chunk-by-chunk with immediate flush
    /// for streaming responses (audio, SSE).
    /// </summary>
    private static async Task ForwardResponse(HttpResponseMessage upstream, HttpResponse client)
    {
        client.StatusCode = (int)upstream.StatusCode;

        var isChunked = upstream.Headers.TransferEncodingChunked == true;
        var contentType = upstream.Content.Headers.ContentType?.MediaType ?? "";
        var isStreaming = isChunked
            || contentType.Contains("text/event-stream")
            || contentType.Contains("application/octet-stream")
            || contentType.Contains("audio/");

        if (isStreaming)
        {
            var bufferingFeature = client.HttpContext.Features
                .Get<Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();
        }

        foreach (var header in upstream.Headers)
        {
            if (HopByHopHeaders.Contains(header.Key)) continue;
            if (header.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;
            if (header.Key.Equals("current-concurrent-requests", StringComparison.OrdinalIgnoreCase)) continue;
            if (header.Key.Equals("maximum-concurrent-requests", StringComparison.OrdinalIgnoreCase)) continue;
            client.Headers[header.Key] = header.Value.ToArray();
        }

        foreach (var header in upstream.Content.Headers)
        {
            if (header.Key.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase)) continue;
            if (isChunked && header.Key.Equals("Content-Length", StringComparison.OrdinalIgnoreCase)) continue;
            client.Headers[header.Key] = header.Value.ToArray();
        }

        await using var upstreamStream = await upstream.Content.ReadAsStreamAsync();

        if (isStreaming)
        {
            var buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = await upstreamStream.ReadAsync(buffer)) > 0)
            {
                await client.Body.WriteAsync(buffer.AsMemory(0, bytesRead));
                await client.Body.FlushAsync();
            }
        }
        else
        {
            await upstreamStream.CopyToAsync(client.Body);
        }
    }

    /// <summary>
    /// Forward a response when the body has already been read as a string.
    /// </summary>
    private static async Task ForwardResponseFromBytes(
        HttpResponseMessage upstream, string body, HttpResponse client)
    {
        client.StatusCode = (int)upstream.StatusCode;

        foreach (var header in upstream.Headers)
        {
            if (HopByHopHeaders.Contains(header.Key)) continue;
            if (header.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase)) continue;
            client.Headers[header.Key] = header.Value.ToArray();
        }

        foreach (var header in upstream.Content.Headers)
        {
            if (header.Key.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase)) continue;
            client.Headers[header.Key] = header.Value.ToArray();
        }

        await client.WriteAsync(body);
    }
}
