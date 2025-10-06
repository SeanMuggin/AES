namespace AES.Evaluator.Models;

public sealed record ScoredEssayRecord(
    string Id,
    string Year,
    string EssayType,
    int? Score,
    int? PredScore,
    string? PredRationale,
    string RunDate,
    string RunId,
    string Model,
    string PromptType,
    string Run
);
