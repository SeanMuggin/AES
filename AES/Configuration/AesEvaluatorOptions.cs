using System;

namespace AES.Evaluator.Configuration;

public sealed class AesEvaluatorOptions
{
    public const string SectionName = "AesEvaluator";

    public enum EvaluatorMode
    {
        ModelTesting,
        Aes
    }

    public EvaluatorMode Mode { get; init; } = EvaluatorMode.ModelTesting;

    public required AzureOpenAiOptions AzureOpenAi { get; init; }
    public required SqlDatabaseOptions SqlDatabase { get; init; }
    public required PromptOptions Prompt { get; init; }
    public required ExecutionOptions Execution { get; init; }

    public sealed class AzureOpenAiOptions
    {
        public string Endpoint { get; init; } = string.Empty;
        public string ApiKey { get; init; } = string.Empty;
        public string ApiVersion { get; init; } = "2025-01-01-preview";
        public string Model { get; init; } = "gpt-5";
    }

    public sealed class SqlDatabaseOptions
    {
        public string ConnectionString { get; init; } = string.Empty;
        public string PredictionsTable { get; init; } = "aes_predictions";
        public string ScoredTable { get; init; } = "aes_scored";
        public string UsageTable { get; init; } = "aes_usage";
        public string MetricsByRubricTable { get; init; } = "aes_metrics_by_rubric";
    }

    public sealed class PromptOptions
    {
        public int ExamplesPerGroup { get; init; }
        public bool IncludeExamples { get; init; }

        public string BuildPromptType(string model)
        {
            if (string.IsNullOrWhiteSpace(model))
            {
                throw new ArgumentException("Model must be provided.", nameof(model));
            }

            var normalizedModel = model.Trim();
            var shotDescriptor = IncludeExamples ? $"{ExamplesPerGroup}Shot" : "0Shot";
            return $"{normalizedModel}_{shotDescriptor}";
        }
    }

    public sealed class ExecutionOptions
    {
        public int MaxBatchSize { get; init; } = 10;
        public int Concurrency { get; init; } = 8;
        public int MaxRetries { get; init; } = 5;
    }
}
