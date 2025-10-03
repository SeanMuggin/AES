namespace AES.Evaluator.Models;

public sealed record UsageLogEntry(
    string Year,
    string EssayType,
    int BatchIndex,
    int Items,
    int? LatencyMs,
    int? InputTokens,
    int? OutputTokens
);
