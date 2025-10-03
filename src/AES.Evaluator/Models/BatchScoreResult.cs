using System.Collections.ObjectModel;
using System.Text.Json;

namespace AES.Evaluator.Models;

public sealed class BatchScoreResult
{
    public BatchScoreResult(
        IReadOnlyDictionary<string, int?> mapping,
        IReadOnlyDictionary<string, string?> rationaleMap,
        int? latencyMs,
        int? inputTokens,
        int? outputTokens,
        IReadOnlyList<JsonElement> raw
    )
    {
        Mapping = new ReadOnlyDictionary<string, int?>(mapping);
        RationaleMap = new ReadOnlyDictionary<string, string?>(rationaleMap);
        LatencyMs = latencyMs;
        InputTokens = inputTokens;
        OutputTokens = outputTokens;
        Raw = new ReadOnlyCollection<JsonElement>(raw);
    }

    public IReadOnlyDictionary<string, int?> Mapping { get; }
    public IReadOnlyDictionary<string, string?> RationaleMap { get; }
    public int? LatencyMs { get; }
    public int? InputTokens { get; }
    public int? OutputTokens { get; }
    public IReadOnlyList<JsonElement> Raw { get; }
}
