using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using AES.Evaluator.Configuration;
using AES.Evaluator.Data;
using AES.Evaluator.Metrics;
using AES.Evaluator.Models;

namespace AES.Evaluator.Services;

public sealed class EssayScoringPipeline
{
    private readonly IDataRepository _dataRepository;
    private readonly AzureOpenAiScorer _scorer;
    private readonly IPipelineResultWriter _resultWriter;
    private readonly AesEvaluatorOptions _options;
    private readonly ExemplarSelector _exemplarSelector;

    public EssayScoringPipeline(
        IDataRepository dataRepository,
        AzureOpenAiScorer scorer,
        IPipelineResultWriter resultWriter,
        AesEvaluatorOptions options,
        int runSeed)
    {
        _dataRepository = dataRepository;
        _scorer = scorer;
        _resultWriter = resultWriter;
        _options = options;
        _exemplarSelector = new ExemplarSelector(runSeed);
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var runDate = DateTime.UtcNow.Date.ToString("yyyy-MM-dd");
        var runId = new Random().Next(1000, 10000).ToString();
        var runName = $"{_options.AzureOpenAi.Model}_{_options.Prompt.PromptType}";

        Console.WriteLine("Loading rubrics and essays...");
        var rubrics = await _dataRepository.GetRubricsAsync(cancellationToken);
        var essays = await _dataRepository.GetEssaysAsync(cancellationToken);
        Console.WriteLine($"Row count in essays: {essays.Count}");
        Console.WriteLine($"Row count in rubric: {rubrics.Count}");

    //    var duplicateCounts = essays
    //.GroupBy(p => p.Id)
    //.ToDictionary(g => g.Key, g => g.Count());

    //    var dupes = duplicateCounts.Where(kvp => kvp.Value > 1).ToList();

        //ValidateRubrics(rubrics);
        //rubrics = RemoveDuplicateRubrics(rubrics);
        if (_options.Execution.MaxBatchSize <= 0)
        {
            throw new InvalidOperationException("MaxBatchSize must be greater than zero.");
        }

        if (_options.Execution.Concurrency <= 0)
        {
            throw new InvalidOperationException("Concurrency must be greater than zero.");
        }

        if (essays.Count == 0)
        {
            Console.WriteLine("No essays to score. Exiting.");
            return;
        }

        var rubricLookup = rubrics.ToDictionary(r => (r.Year, r.EssayType));
        var batches = BuildBatches(essays, rubricLookup).ToList();
        PreviewFirstBatch(batches);

        var predictions = new ConcurrentBag<EssayPrediction>();
        var usage = new ConcurrentBag<UsageLogEntry>();
        var semaphore = new SemaphoreSlim(_options.Execution.Concurrency);
        var tasks = new List<Task>();

        foreach (var batch in batches)
        {
            await semaphore.WaitAsync(cancellationToken);
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    var result = await _scorer.ScoreBatchAsync(batch.Rubric, batch.Items, batch.Exemplars, cancellationToken);
                    foreach (var item in batch.Items)
                    {
                        result.Mapping.TryGetValue(item.Id, out var score);
                        result.RationaleMap.TryGetValue(item.Id, out var rationale);
                        predictions.Add(new EssayPrediction(
                            item.Id,
                            score,
                            rationale,
                            batch.Year,
                            batch.EssayType,
                            batch.BatchIndex,
                            result.LatencyMs,
                            result.InputTokens,
                            result.OutputTokens));
                    }

                    usage.Add(new UsageLogEntry(
                        batch.Year,
                        batch.EssayType,
                        batch.BatchIndex,
                        batch.Items.Count,
                        result.LatencyMs,
                        result.InputTokens,
                        result.OutputTokens));
                }
                finally
                {
                    semaphore.Release();
                }
            }, cancellationToken));
        }

        await Task.WhenAll(tasks);

        var predictionList = predictions.ToList();
        var usageList = usage.ToList();

        var scored = BuildScoredRecords(essays, predictionList, runDate, runId, runName);
        var usageWithRun = usageList.Select(u => new UsageRecordWithRun(
            u.Year,
            u.EssayType,
            u.BatchIndex,
            u.Items,
            u.LatencyMs,
            u.InputTokens,
            u.OutputTokens,
            runDate,
            runId,
            _options.AzureOpenAi.Model,
            _options.Prompt.PromptType,
            runName)).ToList();

        await PersistResultsAsync(scored, usageWithRun, cancellationToken);

        ComputeAndPrintMetrics(scored);

        Console.WriteLine("Done.");
    }

    private void PreviewFirstBatch(IReadOnlyList<BatchContext> batches)
    {
        if (batches.Count == 0)
        {
            return;
        }

        var batch = batches[0];
        var preview = PromptBuilder.BuildUserMessage(batch.Rubric, batch.Items, batch.Exemplars);
        var truncated = preview.Length > 8000 ? preview[..8000] + "... [truncated]" : preview;
        Console.WriteLine(truncated);
    }

    private static void ValidateRubrics(IEnumerable<RubricRecord> rubrics)
    {
        var duplicates = rubrics
            .GroupBy(r => (r.Year, r.EssayType))
            .Where(g => g.Count() > 1)
            .ToList();
        if (duplicates.Count > 0)
        {
            throw new InvalidOperationException("Rubric table has duplicate (Year, EssayType) rows.");
        }
    }

    private IReadOnlyList<BatchContext> BuildBatches(
        IReadOnlyCollection<EssayRecord> essays,
        IReadOnlyDictionary<(string Year, string EssayType), RubricRecord> rubrics)
    {
        var batches = new List<BatchContext>();
        var grouped = essays.GroupBy(e => (e.Year, e.EssayType));
        foreach (var group in grouped)
        {
            if (!rubrics.TryGetValue((group.Key.Year, group.Key.EssayType), out var rubric))
            {
                throw new InvalidOperationException($"Missing rubric for Year='{group.Key.Year}', EssayType='{group.Key.EssayType}'.");
            }

            var groupList = group.ToList();
            var essaysWithRubric = groupList
                .Select(e => new EssayWithRubric(e, rubric.Rubric))
                .ToList();
            var items = essaysWithRubric.Select(e => new BatchItem(e.Id.Trim(), e.EssayContent)).ToList();
            var batchIndex = 0;
            for (var i = 0; i < items.Count; i += _options.Execution.MaxBatchSize)
            {
                var chunk = items.Skip(i).Take(_options.Execution.MaxBatchSize).ToArray();
                IReadOnlyCollection<(string Text, int Score)> exemplars = Array.Empty<(string, int)>();
                if (_options.Prompt.IncludeExamples)
                {
                    var exclude = new HashSet<string>(chunk.Select(c => c.Id));
                    exemplars = _exemplarSelector.ChooseExemplars(essaysWithRubric, exclude, _options.Prompt.ExamplesPerGroup);
                }

                batches.Add(new BatchContext(group.Key.Year, group.Key.EssayType, rubric.Rubric, batchIndex, chunk, exemplars));
                batchIndex++;
            }
        }

        return batches;
    }

    private IReadOnlyList<ScoredEssayRecord> BuildScoredRecords(
        IReadOnlyCollection<EssayRecord> essays,
        IReadOnlyCollection<EssayPrediction> predictions,
        string runDate,
        string runId,
        string runName)
    {
        var predictionLookup = predictions
            .GroupBy(p => p.Id)
            .ToDictionary(g => g.Key, g => g.OrderByDescending(p => p.BatchIndex).First());

        return essays.Select(e =>
        {
            predictionLookup.TryGetValue(e.Id, out var pred);
            return new ScoredEssayRecord(
                e.Id,
                e.Year,
                e.EssayType,
                e.ReaderId,
                e.StudentId,
                e.GoldScore,
                pred?.PredScore,
                pred?.PredRationale,
                runDate,
                runId,
                _options.AzureOpenAi.Model,
                _options.Prompt.PromptType,
                runName);
        }).ToList();
    }

    private async Task PersistResultsAsync(
        IReadOnlyCollection<ScoredEssayRecord> scored,
        IReadOnlyCollection<UsageRecordWithRun> usage,
        CancellationToken cancellationToken)
    {
        await _resultWriter.WritePredictionsAsync(scored, cancellationToken);
        await _resultWriter.WriteUsageAsync(usage, cancellationToken);

        var metrics = BuildMetrics(scored);
        await _resultWriter.WriteMetricsAsync(metrics, cancellationToken);
    }

    private IReadOnlyCollection<MetricSummary> BuildMetrics(IReadOnlyCollection<ScoredEssayRecord> scored)
    {
        var runDate = scored.First().RunDate;
        var runId = scored.First().RunId;
        var model = scored.First().Model;
        var promptType = scored.First().PromptType;
        var run = scored.First().Run;

        var valid = scored
            .Where(s => s.GoldScore.HasValue && s.PredScore.HasValue)
            .Select(s => (Actual: s.GoldScore!.Value, Pred: s.PredScore!.Value, s.Year, s.EssayType))
            .ToList();

        var metricSummaries = new List<MetricSummary>();
        if (valid.Count == 0)
        {
            return metricSummaries;
        }

        foreach (var group in valid.GroupBy(v => (v.Year, v.EssayType)))
        {
            var actual = group.Select(v => v.Actual).ToList();
            var predicted = group.Select(v => v.Pred).ToList();
            metricSummaries.Add(new MetricSummary(
                runDate,
                runId,
                model,
                promptType,
                run,
                group.Key.Year,
                group.Key.EssayType,
                group.Count(),
                Round(MetricsCalculator.Accuracy(actual, predicted)),
                Round(MetricsCalculator.QuadraticWeightedKappa(actual, predicted)),
                Round(MetricsCalculator.MacroF1(actual, predicted)),
                Round(MetricsCalculator.SpearmanCorrelation(actual, predicted))));
        }

        return metricSummaries;
    }

    private static void ComputeAndPrintMetrics(IReadOnlyCollection<ScoredEssayRecord> scored)
    {
        var valid = scored.Where(s => s.GoldScore.HasValue && s.PredScore.HasValue).ToList();
        if (valid.Count == 0)
        {
            Console.WriteLine("No scored essays with both gold and predicted scores; skipping metrics.");
            return;
        }

        var actual = valid.Select(v => v.GoldScore!.Value).ToList();
        var predicted = valid.Select(v => v.PredScore!.Value).ToList();

        var summary = new
        {
            accuracy = Round(MetricsCalculator.Accuracy(actual, predicted)),
            qwk = Round(MetricsCalculator.QuadraticWeightedKappa(actual, predicted)),
            macro_f1 = Round(MetricsCalculator.MacroF1(actual, predicted)),
            spearman_r = Round(MetricsCalculator.SpearmanCorrelation(actual, predicted))
        };

        Console.WriteLine(JsonSerializer.Serialize(summary));

        var matrix = MetricsCalculator.BuildConfusionMatrix(actual, predicted);
        Console.WriteLine("Confusion matrix (rows/cols = 1..5):");
        for (var i = 0; i < matrix.GetLength(0); i++)
        {
            var rowValues = new int[matrix.GetLength(1)];
            for (var j = 0; j < matrix.GetLength(1); j++)
            {
                rowValues[j] = matrix[i, j];
            }

            Console.WriteLine(string.Join(" ", rowValues));
        }
    }

    private static double Round(double value) => double.IsNaN(value) ? double.NaN : Math.Round(value, 4);
    private static List<RubricRecord> RemoveDuplicateRubrics(IEnumerable<RubricRecord> rubrics)
    {
        // Use a dictionary to keep the first occurrence of each (Year, EssayType) pair
        var unique = new Dictionary<(string Year, string EssayType), RubricRecord>();
        foreach(var rubric in rubrics)
        {
            var key = (rubric.Year, rubric.EssayType);
            if(!unique.ContainsKey(key))
            {
                unique[key] = rubric;
            }
        }
        return unique.Values.ToList();
    }

    private sealed record BatchContext(
        string Year,
        string EssayType,
        string Rubric,
        int BatchIndex,
        IReadOnlyList<BatchItem> Items,
        IReadOnlyCollection<(string Text, int Score)> Exemplars);
}
