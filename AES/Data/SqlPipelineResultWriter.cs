using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Azure.Core;
using Microsoft.Data.SqlClient;

namespace AES.Evaluator.Data;

public sealed class SqlPipelineResultWriter : IPipelineResultWriter, IDisposable
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly string _predictionsTableName;
    private readonly string _usageTableName;
    private readonly string _metricsTableName;

    public SqlPipelineResultWriter(AesEvaluatorOptions.SqlDatabaseOptions options, TokenCredential? credential = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        _connectionFactory = new SqlConnectionFactory(options.ConnectionString, credential);
        _predictionsTableName = options.PredictionsTable;
        _usageTableName = options.UsageTable;
        _metricsTableName = options.MetricsByRubricTable;
    }

    public async Task WritePredictionsAsync(IEnumerable<ScoredEssayRecord> predictions, CancellationToken cancellationToken)
    {
        var records = predictions as IReadOnlyList<ScoredEssayRecord> ?? predictions.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var columns = new[]
        {
            new ColumnDefinition<ScoredEssayRecord>("Id", r => r.Id),
            new ColumnDefinition<ScoredEssayRecord>("Year", r => r.Year),
            new ColumnDefinition<ScoredEssayRecord>("EssayType", r => r.EssayType),
            new ColumnDefinition<ScoredEssayRecord>("ReaderId", r => r.ReaderId),
            new ColumnDefinition<ScoredEssayRecord>("StudentId", r => r.StudentId),
            new ColumnDefinition<ScoredEssayRecord>("GoldScore", r => r.GoldScore, SqlDbType.Int),
            new ColumnDefinition<ScoredEssayRecord>("PredScore", r => r.PredScore, SqlDbType.Int),
            new ColumnDefinition<ScoredEssayRecord>("PredRationale", r => r.PredRationale ?? string.Empty),
            new ColumnDefinition<ScoredEssayRecord>("RunDate", r => r.RunDate),
            new ColumnDefinition<ScoredEssayRecord>("RunId", r => r.RunId),
            new ColumnDefinition<ScoredEssayRecord>("Model", r => r.Model),
            new ColumnDefinition<ScoredEssayRecord>("PromptType", r => r.PromptType),
            new ColumnDefinition<ScoredEssayRecord>("Run", r => r.Run)
        };

        await InsertRecordsAsync(records, _predictionsTableName, columns, cancellationToken).ConfigureAwait(false);
    }

    public async Task WriteUsageAsync(IEnumerable<UsageRecordWithRun> usageRecords, CancellationToken cancellationToken)
    {
        var records = usageRecords as IReadOnlyList<UsageRecordWithRun> ?? usageRecords.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var columns = new[]
        {
            new ColumnDefinition<UsageRecordWithRun>("Year", r => r.Year),
            new ColumnDefinition<UsageRecordWithRun>("EssayType", r => r.EssayType),
            new ColumnDefinition<UsageRecordWithRun>("BatchIndex", r => r.BatchIndex, SqlDbType.Int),
            new ColumnDefinition<UsageRecordWithRun>("Items", r => r.Items, SqlDbType.Int),
            new ColumnDefinition<UsageRecordWithRun>("LatencyMs", r => r.LatencyMs, SqlDbType.Int),
            new ColumnDefinition<UsageRecordWithRun>("InputTokens", r => r.InputTokens, SqlDbType.Int),
            new ColumnDefinition<UsageRecordWithRun>("OutputTokens", r => r.OutputTokens, SqlDbType.Int),
            new ColumnDefinition<UsageRecordWithRun>("RunDate", r => r.RunDate),
            new ColumnDefinition<UsageRecordWithRun>("RunId", r => r.RunId),
            new ColumnDefinition<UsageRecordWithRun>("Model", r => r.Model),
            new ColumnDefinition<UsageRecordWithRun>("PromptType", r => r.PromptType),
            new ColumnDefinition<UsageRecordWithRun>("Run", r => r.Run)
        };

        await InsertRecordsAsync(records, _usageTableName, columns, cancellationToken).ConfigureAwait(false);
    }

    public async Task WriteMetricsAsync(IEnumerable<MetricSummary> metrics, CancellationToken cancellationToken)
    {
        var records = metrics as IReadOnlyList<MetricSummary> ?? metrics.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var columns = new[]
        {
            new ColumnDefinition<MetricSummary>("RunDate", r => r.RunDate),
            new ColumnDefinition<MetricSummary>("RunId", r => r.RunId),
            new ColumnDefinition<MetricSummary>("Model", r => r.Model),
            new ColumnDefinition<MetricSummary>("PromptType", r => r.PromptType),
            new ColumnDefinition<MetricSummary>("Run", r => r.Run),
            new ColumnDefinition<MetricSummary>("Year", r => r.Year),
            new ColumnDefinition<MetricSummary>("EssayType", r => r.EssayType),
            new ColumnDefinition<MetricSummary>("count", r => r.Count, SqlDbType.Int),
            new ColumnDefinition<MetricSummary>("accuracy", r => r.Accuracy, SqlDbType.Float),
            new ColumnDefinition<MetricSummary>("qwk", r => r.QuadraticWeightedKappa, SqlDbType.Float),
            new ColumnDefinition<MetricSummary>("macro_f1", r => r.MacroF1, SqlDbType.Float),
            new ColumnDefinition<MetricSummary>("spearman_r", r => r.SpearmanR, SqlDbType.Float)
        };

        await InsertRecordsAsync(records, _metricsTableName, columns, cancellationToken).ConfigureAwait(false);
    }

    private async Task InsertRecordsAsync<T>(IReadOnlyList<T> records, string destinationTable, IReadOnlyList<ColumnDefinition<T>> columns, CancellationToken cancellationToken)
    {
        const int DefaultBatchSize = 100;
        const int MaxParametersPerCommand = 2000;

        if (columns.Count == 0)
        {
            throw new InvalidOperationException("At least one column must be specified for an insert operation.");
        }

        var effectiveBatchSize = Math.Max(1, Math.Min(DefaultBatchSize, MaxParametersPerCommand / columns.Count));
        var columnList = string.Join(", ", columns.Select(c => $"[{c.Name}]"));

        await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        for (var offset = 0; offset < records.Count; offset += effectiveBatchSize)
        {
            var batchSize = Math.Min(effectiveBatchSize, records.Count - offset);
            using var command = connection.CreateCommand();
            var valueClauses = new string[batchSize];

            for (var rowIndex = 0; rowIndex < batchSize; rowIndex++)
            {
                var record = records[offset + rowIndex];
                var parameterNames = new string[columns.Count];

                for (var columnIndex = 0; columnIndex < columns.Count; columnIndex++)
                {
                    var parameterName = $"@p{rowIndex}_{columnIndex}";
                    parameterNames[columnIndex] = parameterName;

                    var value = columns[columnIndex].ValueFactory(record);
                    SqlParameter parameter;
                    if (columns[columnIndex].DbType.HasValue)
                    {
                        parameter = new SqlParameter(parameterName, columns[columnIndex].DbType.Value)
                        {
                            Value = value ?? DBNull.Value
                        };
                    }
                    else
                    {
                        parameter = new SqlParameter(parameterName, value ?? DBNull.Value);
                    }

                    command.Parameters.Add(parameter);
                }

                valueClauses[rowIndex] = $"({string.Join(", ", parameterNames)})";
            }

            var values = string.Join(", ", valueClauses);
            command.CommandText = $"INSERT INTO {destinationTable} ({columnList}) VALUES {values}";

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed record ColumnDefinition<T>(string Name, Func<T, object?> ValueFactory, SqlDbType? DbType = null);

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
}
