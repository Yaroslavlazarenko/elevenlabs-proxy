// ============================================================================
// KeyPool.cs — Thread-safe round-robin API key pool with cooldown
//
// How it works:
//   - Keys are stored in a fixed-size array; an atomic counter provides
//     lock-free round-robin rotation across concurrent requests.
//   - When ElevenLabs responds with 429, the key is "cooled down" for a
//     configurable period (or the Retry-After value from the response).
//     During cooldown the key is skipped and the next available one is used.
//   - If ALL keys are on cooldown, the caller gets null + the number of
//     milliseconds until the soonest key recovers (so the proxy can return
//     a meaningful Retry-After to the client).
// ============================================================================

using System.Collections.Concurrent;

namespace ElevenLabsProxy;

/// <summary>
/// Thread-safe round-robin key pool with per-key cooldown support.
/// When a key gets a 429, it is put on cooldown and skipped until recovery.
/// </summary>
public class KeyPool
{
    private readonly KeyEntry[] _keys;
    private int _index; // atomically incremented for round-robin

    /// <summary>Total number of keys in the pool.</summary>
    public int Size => _keys.Length;

    /// <summary>Number of keys not currently on cooldown.</summary>
    public int AvailableCount
    {
        get
        {
            var now = DateTimeOffset.UtcNow;
            return _keys.Count(k => k.AvailableAt <= now);
        }
    }

    /// <param name="keys">Non-empty list of ElevenLabs API keys.</param>
    /// <exception cref="ArgumentException">Thrown when the list is empty.</exception>
    public KeyPool(IReadOnlyList<string> keys)
    {
        if (keys.Count == 0)
            throw new ArgumentException("At least one API key is required.", nameof(keys));

        _keys = keys.Select(k => new KeyEntry(k)).ToArray();
    }

    /// <summary>
    /// Acquire the next available key via round-robin.
    /// </summary>
    /// <param name="retryAfterMs">
    /// When no key is available, set to the number of milliseconds until
    /// the soonest key comes off cooldown.
    /// </param>
    /// <returns>API key string, or <c>null</c> if all keys are on cooldown.</returns>
    public string? Acquire(out long retryAfterMs)
    {
        var now = DateTimeOffset.UtcNow;
        var len = _keys.Length;

        // Scan all keys starting from the current round-robin position.
        // Interlocked.Add ensures thread-safe rotation without locks.
        for (int i = 0; i < len; i++)
        {
            var idx = (Interlocked.Add(ref _index, 1) - 1) % len;
            if (idx < 0) idx += len; // guard against int overflow wrapping negative
            var entry = _keys[idx];

            if (entry.AvailableAt <= now)
            {
                retryAfterMs = 0;
                return entry.Key;
            }
        }

        // Every key is on cooldown — report when the first one recovers
        var soonest = _keys.Min(k => k.AvailableAt);
        var diff = soonest - now;
        retryAfterMs = Math.Max(0, (long)diff.TotalMilliseconds);
        return null;
    }

    /// <summary>
    /// Put a key on cooldown after receiving a 429 from ElevenLabs.
    /// </summary>
    /// <param name="key">The API key that was rate-limited.</param>
    /// <param name="retryAfterMs">
    /// Value from the upstream Retry-After header (converted to ms), if present.
    /// </param>
    /// <param name="defaultCooldownMs">
    /// Fallback cooldown when no Retry-After header was provided.
    /// </param>
    public void MarkRateLimited(string key, int? retryAfterMs = null, int defaultCooldownMs = 60_000)
    {
        var entry = _keys.FirstOrDefault(k => k.Key == key);
        if (entry == null) return;

        // Prefer the server-provided Retry-After; fall back to the configured default
        var cooldown = retryAfterMs is > 0 ? retryAfterMs.Value : defaultCooldownMs;
        entry.AvailableAt = DateTimeOffset.UtcNow.AddMilliseconds(cooldown);

        Console.WriteLine(
            $"[key-pool] Key ...{key[^6..]} on cooldown for {cooldown}ms " +
            $"(available: {AvailableCount}/{Size})");
    }

    /// <summary>
    /// Move a key to the end of the pool array so it is tried last
    /// in the round-robin rotation. Used for 402 (quota exceeded) —
    /// the key still works for smaller requests but shouldn't be the
    /// first one picked.
    /// </summary>
    public void SendToBack(string key)
    {
        // Find the key's current position
        var idx = Array.FindIndex(_keys, k => k.Key == key);
        if (idx < 0 || idx == _keys.Length - 1) return;

        // Shift everything after it one position left, put the key at the end
        var entry = _keys[idx];
        Array.Copy(_keys, idx + 1, _keys, idx, _keys.Length - idx - 1);
        _keys[^1] = entry;

        // Reset round-robin index to 0 so the next Acquire() starts from
        // the beginning of the reshuffled array
        Interlocked.Exchange(ref _index, 0);

        Console.WriteLine(
            $"[key-pool] Key ...{key[^6..]} moved to back of queue");
    }

    /// <summary>
    /// Snapshot of every key's status — used by the /health endpoint.
    /// Key values are masked (only last 6 chars shown) for security.
    /// </summary>
    public IReadOnlyList<KeyStatus> GetStatus()
    {
        var now = DateTimeOffset.UtcNow;
        return _keys.Select((e, i) => new KeyStatus
        {
            Index = i,
            KeySuffix = $"...{e.Key[^6..]}",
            Available = e.AvailableAt <= now,
            CooldownRemainingMs = e.AvailableAt > now
                ? (long)(e.AvailableAt - now).TotalMilliseconds
                : 0
        }).ToArray();
    }

    // ── Internal key wrapper ───────────────────────────────────────────
    private class KeyEntry
    {
        public string Key { get; }

        /// <summary>
        /// UTC timestamp after which this key can be used again.
        /// <c>DateTimeOffset.MinValue</c> means "available immediately".
        /// </summary>
        public DateTimeOffset AvailableAt { get; set; } = DateTimeOffset.MinValue;

        public KeyEntry(string key) => Key = key;
    }

    /// <summary>DTO exposed by <see cref="GetStatus"/> for the health endpoint.</summary>
    public class KeyStatus
    {
        public int Index { get; set; }
        public string KeySuffix { get; set; } = string.Empty;
        public bool Available { get; set; }
        public long CooldownRemainingMs { get; set; }
    }
}
