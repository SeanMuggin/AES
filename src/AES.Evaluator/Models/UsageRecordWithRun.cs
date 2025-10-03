namespace AES.Evaluator.Models;

public sealed record UsageRecordWithRun(
    string Year,
    string EssayType,
    int BatchIndex,
    int Items,
    int? LatencyMs,
    int? InputTokens,
    int? OutputTokens,
    string RunDate,
    string RunId,
    string Model,
    string PromptType,
    string Run
);
