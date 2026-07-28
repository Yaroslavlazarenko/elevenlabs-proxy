// ============================================================================
// ProxySettings.cs — Configuration model
//
// All values are populated from environment variables in Program.cs.
// Defaults are tuned for typical ElevenLabs usage patterns.
// ============================================================================

namespace ElevenLabsProxy;

public class ProxySettings
{
    /// <summary>
    /// Secret key that clients must send in the <c>xi-api-key</c> header
    /// to authenticate with the proxy. This is NOT a real ElevenLabs key —
    /// it is a separate secret shared between the proxy and its consumers.
    /// </summary>
    public string ProxyApiKey { get; set; } = string.Empty;

    /// <summary>
    /// Base URL of the ElevenLabs API (without trailing slash).
    /// Override for regional endpoints, e.g. <c>https://api.eu.residency.elevenlabs.io</c>.
    /// </summary>
    public string ElevenLabsBaseUrl { get; set; } = "https://api.elevenlabs.io";

    /// <summary>
    /// Pool of real ElevenLabs API keys. The proxy rotates through them
    /// in round-robin order and puts rate-limited ones on cooldown.
    /// </summary>
    public List<string> ElevenLabsApiKeys { get; set; } = new();

    /// <summary>
    /// Maximum number of retry attempts when the upstream returns 500 or 503.
    /// Also limits the total number of key-rotation attempts on 429.
    /// </summary>
    public int RetryCount { get; set; } = 3;

    /// <summary>
    /// Delay in milliseconds between retries for 500/503 server errors.
    /// </summary>
    public int RetryDelayMs { get; set; } = 1000;

    /// <summary>
    /// Default cooldown period in milliseconds for a key that received a 429.
    /// If the upstream response includes a <c>Retry-After</c> header, that
    /// value takes precedence over this default.
    /// </summary>
    public int KeyCooldownMs { get; set; } = 60_000;

    /// <summary>
    /// Timeout in seconds for each individual upstream request.
    /// TTS generation can take a while for long texts, so the default is generous.
    /// </summary>
    public int RequestTimeoutSeconds { get; set; } = 120;
}
