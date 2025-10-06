namespace AES.Evaluator.Models;

public sealed record EssayRecord(
    string Id,
    string Year,
    string EssayType,
    string EssayContent,
    int? Score
);
