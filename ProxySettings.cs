namespace ElevenLabsProxy;

public class ProxySettings
{
    /// <summary>
    /// Secret key that clients must send in the xi-api-key header to authenticate with the proxy.
    /// </summary>
    public string ProxyApiKey { get; set; } = string.Empty;

    /// <summary>
    /// Base URL of the ElevenLabs API.
    /// </summary>
    public string ElevenLabsBaseUrl { get; set; } = "https://api.elevenlabs.io";

    /// <summary>
    /// Pool of real ElevenLabs API keys.
    /// </summary>
    public List<string> ElevenLabsApiKeys { get; set; } = new();

    /// <summary>
    /// Number of retry attempts for 500/503 errors.
    /// </summary>
    public int RetryCount { get; set; } = 3;

    /// <summary>
    /// Delay between retries for 500/503 in milliseconds.
    /// </summary>
    public int RetryDelayMs { get; set; } = 1000;

    /// <summary>
    /// Default cooldown period for rate-limited keys (ms).
    /// Used when no Retry-After header is present.
    /// </summary>
    public int KeyCooldownMs { get; set; } = 60_000;

    /// <summary>
    /// Upstream request timeout in seconds.
    /// </summary>
    public int RequestTimeoutSeconds { get; set; } = 120;
}
