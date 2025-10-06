namespace AES.Evaluator.Configuration;

public sealed class AesEvaluatorOptions
{
    public const string SectionName = "AesEvaluator";

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
        public string UsageTable { get; init; } = "aes_usage";
        public string MetricsByRubricTable { get; init; } = "aes_metrics_by_rubric";
        public string EssaysTable { get; init; } = "essays";
        public string RubricsTable { get; init; } = "rubric";
    }

    public sealed class PromptOptions
    {
        public string PromptType { get; init; } = "3Shot_Msg2";
        public int ExamplesPerGroup { get; init; }
        public bool IncludeExamples { get; init; }
    }

    public sealed class ExecutionOptions
    {
        public int MaxBatchSize { get; init; } = 10;
        public int Concurrency { get; init; } = 8;
        public int MaxRetries { get; init; } = 5;
    }
}
