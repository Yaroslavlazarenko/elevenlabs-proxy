// ============================================================================
// KeyPool.cs — Thread-safe API key pool with concurrency control
//
// Features:
//   - Round-robin rotation with per-key concurrency limits
//   - Cooldown support for 429 rate-limited keys
//   - Disabled keys for banned/unusual_activity keys
//   - SendToBack for quota-exceeded keys
//   - Acquire/Release pattern to track in-flight requests per key
//   - Spreads parallel requests across different keys automatically
// ============================================================================

namespace ElevenLabsProxy;

public class KeyPool
{
    private readonly KeyEntry[] _keys;
    private readonly int _maxConcurrentPerKey;
    private int _index; // atomically incremented for round-robin
    private readonly object _lock = new();

    public int Size => _keys.Length;

    /// <summary>Number of keys not on cooldown and not disabled.</summary>
    public int AvailableCount
    {
        get
        {
            var now = DateTimeOffset.UtcNow;
            return _keys.Count(k => !k.Disabled && k.AvailableAt <= now);
        }
    }

    /// <param name="keys">Non-empty list of ElevenLabs API keys.</param>
    /// <param name="maxConcurrentPerKey">Max parallel requests per key (free tier = 4).</param>
    public KeyPool(IReadOnlyList<string> keys, int maxConcurrentPerKey = 4)
    {
        if (keys.Count == 0)
            throw new ArgumentException("At least one API key is required.", nameof(keys));

        _maxConcurrentPerKey = maxConcurrentPerKey;
        _keys = keys.Select(k => new KeyEntry(k)).ToArray();
    }

    /// <summary>
    /// Acquire the next available key that has free concurrency slots.
    /// Round-robin rotation ensures different users hit different keys.
    /// </summary>
    /// <param name="retryAfterMs">
    /// When no key is available, ms until the soonest key comes off cooldown.
    /// </param>
    /// <returns>API key string, or <c>null</c> if no key is available.</returns>
    public string? Acquire(out long retryAfterMs)
    {
        var now = DateTimeOffset.UtcNow;
        var len = _keys.Length;

        lock (_lock)
        {
            // Scan all keys starting from the current round-robin position
            for (int i = 0; i < len; i++)
            {
                var idx = _index % len;
                _index++;
                var entry = _keys[idx];

                // Skip disabled keys (banned by ElevenLabs)
                if (entry.Disabled) continue;

                // Skip keys on cooldown
                if (entry.AvailableAt > now) continue;

                // Skip keys at concurrency limit
                if (entry.InFlight >= _maxConcurrentPerKey) continue;

                entry.InFlight++;
                retryAfterMs = 0;
                return entry.Key;
            }

            // No key available — find soonest recovery among non-disabled keys
            var activeKeys = _keys.Where(k => !k.Disabled).ToArray();
            if (activeKeys.Length == 0)
            {
                retryAfterMs = 0;
                return null;
            }

            // If all keys are just at concurrency limit (not cooldown), retry soon
            var allAtConcurrencyLimit = activeKeys.All(k => k.AvailableAt <= now && k.InFlight >= _maxConcurrentPerKey);
            if (allAtConcurrencyLimit)
            {
                retryAfterMs = 100; // suggest retry in 100ms
                return null;
            }

            var soonest = activeKeys.Min(k => k.AvailableAt);
            var diff = soonest - now;
            retryAfterMs = Math.Max(0, (long)diff.TotalMilliseconds);
            return null;
        }
    }

    /// <summary>
    /// Release a key after a request completes (success or failure).
    /// Must be called for every successful <see cref="Acquire"/> to free
    /// the concurrency slot.
    /// </summary>
    public void Release(string key)
    {
        lock (_lock)
        {
            var entry = _keys.FirstOrDefault(k => k.Key == key);
            if (entry != null && entry.InFlight > 0)
                entry.InFlight--;
        }
    }

    /// <summary>
    /// Put a key on cooldown after receiving a 429 from ElevenLabs.
    /// The concurrency slot is also released.
    /// </summary>
    public void MarkRateLimited(string key, int? retryAfterMs = null, int defaultCooldownMs = 60_000)
    {
        lock (_lock)
        {
            var entry = _keys.FirstOrDefault(k => k.Key == key);
            if (entry == null) return;

            var cooldown = retryAfterMs is > 0 ? retryAfterMs.Value : defaultCooldownMs;
            entry.AvailableAt = DateTimeOffset.UtcNow.AddMilliseconds(cooldown);

            Console.WriteLine(
                $"[key-pool] Key ...{key[^6..]} on cooldown for {cooldown}ms " +
                $"(available: {AvailableCount}/{Size})");
        }
    }

    /// <summary>
    /// Permanently disable a key (e.g. detected_unusual_activity ban).
    /// The key will never be returned by <see cref="Acquire"/> again.
    /// </summary>
    public void Disable(string key)
    {
        lock (_lock)
        {
            var entry = _keys.FirstOrDefault(k => k.Key == key);
            if (entry == null) return;

            entry.Disabled = true;
            entry.InFlight = 0;

            Console.WriteLine(
                $"[key-pool] Key ...{key[^6..]} DISABLED " +
                $"(available: {AvailableCount}/{Size})");
        }
    }

    /// <summary>
    /// Move a key to the end of the pool array so it is tried last.
    /// Used for quota_exceeded — the key may still work for smaller requests.
    /// </summary>
    public void SendToBack(string key)
    {
        lock (_lock)
        {
            var idx = Array.FindIndex(_keys, k => k.Key == key);
            if (idx < 0 || idx == _keys.Length - 1) return;

            var entry = _keys[idx];
            Array.Copy(_keys, idx + 1, _keys, idx, _keys.Length - idx - 1);
            _keys[^1] = entry;

            _index = 0;

            Console.WriteLine(
                $"[key-pool] Key ...{key[^6..]} moved to back of queue");
        }
    }

    /// <summary>
    /// Snapshot of every key's status for the /health endpoint.
    /// </summary>
    public IReadOnlyList<KeyStatus> GetStatus()
    {
        var now = DateTimeOffset.UtcNow;
        lock (_lock)
        {
            return _keys.Select((e, i) => new KeyStatus
            {
                Index = i,
                KeySuffix = $"...{e.Key[^6..]}",
                Available = !e.Disabled && e.AvailableAt <= now,
                Disabled = e.Disabled,
                InFlight = e.InFlight,
                MaxConcurrent = _maxConcurrentPerKey,
                CooldownRemainingMs = e.AvailableAt > now
                    ? (long)(e.AvailableAt - now).TotalMilliseconds
                    : 0
            }).ToArray();
        }
    }

    // ── Internal key wrapper ───────────────────────────────────────────
    private class KeyEntry
    {
        public string Key { get; }
        public DateTimeOffset AvailableAt { get; set; } = DateTimeOffset.MinValue;
        public int InFlight { get; set; }
        public bool Disabled { get; set; }

        public KeyEntry(string key) => Key = key;
    }

    public class KeyStatus
    {
        public int Index { get; set; }
        public string KeySuffix { get; set; } = string.Empty;
        public bool Available { get; set; }
        public bool Disabled { get; set; }
        public int InFlight { get; set; }
        public int MaxConcurrent { get; set; }
        public long CooldownRemainingMs { get; set; }
    }
}
