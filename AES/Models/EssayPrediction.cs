namespace AES.Evaluator.Models;

public sealed record EssayPrediction(
    string Id,
    int? PredScore,
    string? PredRationale,
    string Year,
    string EssayType,
    int BatchIndex,
    int? LatencyMs,
    int? InputTokens,
    int? OutputTokens
);
