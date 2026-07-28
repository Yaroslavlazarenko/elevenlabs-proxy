using System.Collections.Concurrent;

namespace ElevenLabsProxy;

/// <summary>
/// Thread-safe round-robin key pool with per-key cooldown support.
/// When a key gets a 429, it is put on cooldown and skipped until recovery.
/// </summary>
public class KeyPool
{
    private readonly KeyEntry[] _keys;
    private int _index;

    public int Size => _keys.Length;

    public int AvailableCount
    {
        get
        {
            var now = DateTimeOffset.UtcNow;
            return _keys.Count(k => k.AvailableAt <= now);
        }
    }

    public KeyPool(IReadOnlyList<string> keys)
    {
        if (keys.Count == 0)
            throw new ArgumentException("At least one API key is required.", nameof(keys));

        _keys = keys.Select(k => new KeyEntry(k)).ToArray();
    }

    /// <summary>
    /// Acquire the next available key via round-robin.
    /// Returns the key string, or null if all keys are on cooldown.
    /// When null, <paramref name="retryAfterMs"/> indicates when the soonest key recovers.
    /// </summary>
    public string? Acquire(out long retryAfterMs)
    {
        var now = DateTimeOffset.UtcNow;
        var len = _keys.Length;

        // Round-robin: start from current index, wrap around
        for (int i = 0; i < len; i++)
        {
            var idx = (Interlocked.Add(ref _index, 1) - 1) % len;
            if (idx < 0) idx += len; // handle overflow
            var entry = _keys[idx];

            if (entry.AvailableAt <= now)
            {
                retryAfterMs = 0;
                return entry.Key;
            }
        }

        // All on cooldown — find the soonest recovery
        var soonest = _keys.Min(k => k.AvailableAt);
        var diff = soonest - now;
        retryAfterMs = Math.Max(0, (long)diff.TotalMilliseconds);
        return null;
    }

    /// <summary>
    /// Put a key on cooldown after receiving a 429.
    /// </summary>
    public void MarkRateLimited(string key, int? retryAfterMs = null, int defaultCooldownMs = 60_000)
    {
        var entry = _keys.FirstOrDefault(k => k.Key == key);
        if (entry == null) return;

        var cooldown = retryAfterMs is > 0 ? retryAfterMs.Value : defaultCooldownMs;
        entry.AvailableAt = DateTimeOffset.UtcNow.AddMilliseconds(cooldown);

        Console.WriteLine(
            $"[key-pool] Key ...{key[^6..]} on cooldown for {cooldown}ms " +
            $"(available: {AvailableCount}/{Size})");
    }

    /// <summary>
    /// Return status of all keys for the health endpoint.
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

    private class KeyEntry
    {
        public string Key { get; }
        public DateTimeOffset AvailableAt { get; set; } = DateTimeOffset.MinValue;

        public KeyEntry(string key) => Key = key;
    }

    public class KeyStatus
    {
        public int Index { get; set; }
        public string KeySuffix { get; set; } = string.Empty;
        public bool Available { get; set; }
        public long CooldownRemainingMs { get; set; }
    }
}
