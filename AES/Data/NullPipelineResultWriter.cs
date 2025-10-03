using System.Linq;
using AES.Evaluator.Models;

namespace AES.Evaluator.Data;

public sealed class NullPipelineResultWriter : IPipelineResultWriter
{
    private readonly Action<string>? _logger;

    public NullPipelineResultWriter(Action<string>? logger = null)
    {
        _logger = logger;
    }

    public Task WritePredictionsAsync(IEnumerable<ScoredEssayRecord> predictions, CancellationToken cancellationToken)
    {
        _logger?.Invoke($"Skipping persistence of {predictions.Count()} predictions (no writer configured).");
        return Task.CompletedTask;
    }

    public Task WriteUsageAsync(IEnumerable<UsageRecordWithRun> usageRecords, CancellationToken cancellationToken)
    {
        _logger?.Invoke($"Skipping persistence of {usageRecords.Count()} usage rows (no writer configured).");
        return Task.CompletedTask;
    }

    public Task WriteMetricsAsync(IEnumerable<MetricSummary> metrics, CancellationToken cancellationToken)
    {
        _logger?.Invoke($"Skipping persistence of {metrics.Count()} metrics rows (no writer configured).");
        return Task.CompletedTask;
    }
}
