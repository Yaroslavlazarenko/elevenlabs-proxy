// ============================================================================
// WebSocketProxyMiddleware.cs — WebSocket proxy for ElevenLabs real-time APIs
// With seamless hot-failover on quota_exceeded (STT/TTS stream survival)
// ============================================================================

using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace ElevenLabsProxy;

public class WebSocketProxyMiddleware
{
    private readonly RequestDelegate _next;
    private readonly KeyPool _keyPool;
    private readonly ProxySettings _settings;
    private readonly ILogger<WebSocketProxyMiddleware> _logger;

    private const int BufferSize = 8192;

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

    private record QueuedFrame(byte[] Data, WebSocketMessageType MessageType, bool EndOfMessage);

    public async Task InvokeAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            await _next(context);
            return;
        }

        _logger.LogInformation("[ws-proxy] WebSocket upgrade for {Path}", context.Request.Path);

        // 1. Accept client WebSocket first so the client connection is stable
        using var clientWs = await context.WebSockets.AcceptWebSocketAsync();

        var upstreamUriTemplate = BuildUpstreamUri(context);

        // 2. Acquire initial key and connect upstream
        string? currentApiKey = null;
        ClientWebSocket? currentUpstreamWs = null;

        var triedKeys = new HashSet<string>();
        while (currentUpstreamWs == null)
        {
            currentApiKey = _keyPool.Acquire(out var retryAfterMs);
            if (currentApiKey == null || triedKeys.Count >= _keyPool.Size)
            {
                _logger.LogWarning("[ws-proxy] All keys on cooldown or exhausted at initial connect");
                await clientWs.CloseAsync(WebSocketCloseStatus.InternalServerError, "All API keys rate-limited or exhausted", CancellationToken.None);
                return;
            }

            triedKeys.Add(currentApiKey);
            currentUpstreamWs = await ConnectUpstreamAsync(currentApiKey, upstreamUriTemplate, context);
            if (currentUpstreamWs == null)
            {
                _keyPool.SendToBack(currentApiKey);
            }
        }

        _logger.LogInformation("[ws-proxy] Upstream connected with key ...{Key}. Relaying frames with hot-failover support.", currentApiKey?[^6..] ?? "unknown");

        // Channel for client -> upstream message queue (handles buffering during failover)
        var clientChannel = Channel.CreateBounded<QueuedFrame>(new BoundedChannelOptions(150)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });

        using var cts = new CancellationTokenSource();
        var upstreamLock = new SemaphoreSlim(1, 1);

        // Task 1: Client -> Channel reader
        var clientReaderTask = Task.Run(async () =>
        {
            var buffer = new byte[BufferSize];
            try
            {
                while (!cts.Token.IsCancellationRequested && clientWs.State == WebSocketState.Open)
                {
                    var result = await clientWs.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogDebug("[ws-proxy] Client sent close frame");
                        break;
                    }

                    var frameData = new byte[result.Count];
                    Array.Copy(buffer, frameData, result.Count);
                    await clientChannel.Writer.WriteAsync(new QueuedFrame(frameData, result.MessageType, result.EndOfMessage), cts.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogDebug("[ws-proxy] Client reader exited: {Message}", ex.Message);
            }
            finally
            {
                clientChannel.Writer.TryComplete();
                cts.Cancel();
            }
        });

        // Task 2: Channel -> Upstream writer
        var upstreamWriterTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var frame in clientChannel.Reader.ReadAllAsync(cts.Token))
                {
                    await upstreamLock.WaitAsync(cts.Token);
                    try
                    {
                        if (currentUpstreamWs != null && currentUpstreamWs.State == WebSocketState.Open)
                        {
                            await currentUpstreamWs.SendAsync(
                                new ArraySegment<byte>(frame.Data),
                                frame.MessageType,
                                frame.EndOfMessage,
                                cts.Token
                            );
                        }
                    }
                    finally
                    {
                        upstreamLock.Release();
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogDebug("[ws-proxy] Upstream writer exited: {Message}", ex.Message);
            }
        });

        // Task 3: Upstream -> Client reader with quota failover
        var upstreamReaderTask = Task.Run(async () =>
        {
            var buffer = new byte[BufferSize];
            var textMessageAccumulator = new MemoryStream();

            try
            {
                while (!cts.Token.IsCancellationRequested && clientWs.State == WebSocketState.Open)
                {
                    if (currentUpstreamWs == null || currentUpstreamWs.State != WebSocketState.Open)
                    {
                        break;
                    }

                    WebSocketReceiveResult result;
                    try
                    {
                        result = await currentUpstreamWs.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);
                    }
                    catch (Exception ex) when (!cts.Token.IsCancellationRequested)
                    {
                        _logger.LogWarning("[ws-proxy] Upstream receive error: {Message}", ex.Message);
                        break;
                    }

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogDebug("[ws-proxy] Upstream closed connection");
                        break;
                    }

                    // Check for quota_exceeded in text frames
                    if (result.MessageType == WebSocketMessageType.Text)
                    {
                        textMessageAccumulator.Write(buffer, 0, result.Count);

                        if (result.EndOfMessage)
                        {
                            var text = Encoding.UTF8.GetString(textMessageAccumulator.ToArray());
                            textMessageAccumulator.SetLength(0);

                            if (text.Contains("quota_exceeded", StringComparison.OrdinalIgnoreCase) ||
                                text.Contains("You have exceeded your quota", StringComparison.OrdinalIgnoreCase))
                            {
                                _logger.LogWarning("[ws-proxy] Upstream key ...{Key} quota exceeded mid-session! Initiating hot-failover...", currentApiKey?[^6..]);
                                
                                bool failedOver = false;
                                await upstreamLock.WaitAsync(cts.Token);
                                try
                                {
                                    if (currentApiKey != null)
                                    {
                                        _keyPool.SendToBack(currentApiKey);
                                        _keyPool.MarkRateLimited(currentApiKey, defaultCooldownMs: 3600_000);
                                    }

                                    try { currentUpstreamWs.Dispose(); } catch { }
                                    currentUpstreamWs = null;

                                    // Try connecting with remaining keys
                                    int failoverAttempts = 0;
                                    while (failoverAttempts < _keyPool.Size)
                                    {
                                        failoverAttempts++;
                                        var nextKey = _keyPool.Acquire(out _);
                                        if (nextKey == null) break;

                                        var newWs = await ConnectUpstreamAsync(nextKey, upstreamUriTemplate, context);
                                        if (newWs != null)
                                        {
                                            currentApiKey = nextKey;
                                            currentUpstreamWs = newWs;
                                            failedOver = true;
                                            _logger.LogInformation("[ws-proxy] Hot-failover SUCCESS! Switched to key ...{Key}", nextKey[^6..]);

                                            // Consume initial session_started from new upstream silently
                                            try
                                            {
                                                var initBuf = new byte[4096];
                                                using var initCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                                                var initRes = await newWs.ReceiveAsync(new ArraySegment<byte>(initBuf), initCts.Token);
                                                _logger.LogDebug("[ws-proxy] Suppressed duplicate session_started from failover upstream");
                                            }
                                            catch { }

                                            break;
                                        }
                                        else
                                        {
                                            _keyPool.SendToBack(nextKey);
                                        }
                                    }
                                }
                                finally
                                {
                                    upstreamLock.Release();
                                }

                                if (failedOver)
                                {
                                    // Successfully switched to new upstream! Continue loop with new socket!
                                    continue;
                                }
                                else
                                {
                                    _logger.LogError("[ws-proxy] Failover failed: no other working keys available. Forwarding error to client.");
                                    var errBytes = Encoding.UTF8.GetBytes(text);
                                    await clientWs.SendAsync(new ArraySegment<byte>(errBytes), WebSocketMessageType.Text, true, CancellationToken.None);
                                    break;
                                }
                            }
                            else
                            {
                                // Normal text message: forward full text to client
                                var textBytes = Encoding.UTF8.GetBytes(text);
                                await clientWs.SendAsync(new ArraySegment<byte>(textBytes), WebSocketMessageType.Text, true, cts.Token);
                                continue;
                            }
                        }
                        else
                        {
                            // Partial text chunk, wait for EndOfMessage
                            continue;
                        }
                    }

                    // Forward binary frames as-is
                    await clientWs.SendAsync(new ArraySegment<byte>(buffer, 0, result.Count), result.MessageType, result.EndOfMessage, cts.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogDebug("[ws-proxy] Upstream reader exited: {Message}", ex.Message);
            }
            finally
            {
                cts.Cancel();
            }
        });

        // Wait until any task finishes
        await Task.WhenAny(clientReaderTask, upstreamWriterTask, upstreamReaderTask);
        cts.Cancel();

        // Cleanup
        try { await Task.WhenAll(clientReaderTask, upstreamWriterTask, upstreamReaderTask); }
        catch { }

        await upstreamLock.WaitAsync();
        try
        {
            if (currentUpstreamWs != null && currentUpstreamWs.State == WebSocketState.Open)
            {
                try { await currentUpstreamWs.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None); } catch { }
            }
        }
        finally
        {
            upstreamLock.Release();
            upstreamLock.Dispose();
            currentUpstreamWs?.Dispose();
        }

        if (clientWs.State == WebSocketState.Open)
        {
            try { await clientWs.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Session finished", CancellationToken.None); } catch { }
        }

        _logger.LogInformation("[ws-proxy] WebSocket connection closed for {Path}", context.Request.Path);
    }

    private Uri BuildUpstreamUri(HttpContext context)
    {
        var baseUrl = _settings.ElevenLabsBaseUrl
            .Replace("https://", "wss://")
            .Replace("http://", "ws://")
            .TrimEnd('/');

        var queryParams = context.Request.Query
            .Where(q => !q.Key.Equals("xi-api-key", StringComparison.OrdinalIgnoreCase))
            .Select(q => $"{Uri.EscapeDataString(q.Key)}={Uri.EscapeDataString(q.Value.ToString())}")
            .ToList();

        return new Uri($"{baseUrl}{context.Request.Path}?{string.Join("&", queryParams)}");
    }

    private async Task<ClientWebSocket?> ConnectUpstreamAsync(string apiKey, Uri upstreamUri, HttpContext context)
    {
        var upstreamWs = new ClientWebSocket();
        upstreamWs.Options.SetRequestHeader("xi-api-key", apiKey);

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
            catch { }
        }

        try
        {
            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await upstreamWs.ConnectAsync(upstreamUri, connectCts.Token);
            return upstreamWs;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[ws-proxy] Failed to connect to upstream with key ...{Key}: {Message}", apiKey[^6..], ex.Message);
            upstreamWs.Dispose();
            return null;
        }
    }
}
