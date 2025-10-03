using AES.Evaluator.Models;

namespace AES.Evaluator.Data;

public interface IPipelineResultWriter
{
    Task WritePredictionsAsync(IEnumerable<ScoredEssayRecord> predictions, CancellationToken cancellationToken);
    Task WriteUsageAsync(IEnumerable<UsageRecordWithRun> usageRecords, CancellationToken cancellationToken);
    Task WriteMetricsAsync(IEnumerable<MetricSummary> metrics, CancellationToken cancellationToken);
}
