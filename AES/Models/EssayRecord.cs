namespace AES.Evaluator.Models;

public sealed record EssayRecord(
    string Id,
    string Year,
    string EssayType,
    string EssayContent,
    string? ReaderId,
    string? StudentId,
    int? GoldScore
);
