namespace AES.Evaluator.Models;

public sealed record MetricSummary(
    string RunDate,
    string RunId,
    string Model,
    string PromptType,
    string Run,
    string Year,
    string EssayType,
    int Count,
    double Accuracy,
    double QuadraticWeightedKappa,
    double MacroF1,
    double SpearmanR
);
