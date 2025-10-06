using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
        var records = predictions as IReadOnlyCollection<ScoredEssayRecord> ?? predictions.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var table = new DataTable();
        table.Columns.Add("Id", typeof(string));
        table.Columns.Add("Year", typeof(string));
        table.Columns.Add("EssayType", typeof(string));
        table.Columns.Add("Score", typeof(int));
        table.Columns.Add("PredScore", typeof(int));
        table.Columns.Add("PredRationale", typeof(string));
        table.Columns.Add("RunDate", typeof(string));
        table.Columns.Add("RunId", typeof(string));
        table.Columns.Add("Model", typeof(string));
        table.Columns.Add("PromptType", typeof(string));
        table.Columns.Add("Run", typeof(string));

        foreach (var record in records)
        {
            table.Rows.Add(
                record.Id,
                record.Year,
                record.EssayType,
                record.Score ?? (object)DBNull.Value,
                record.PredScore ?? (object)DBNull.Value,
                record.PredRationale ?? string.Empty,
                record.RunDate,
                record.RunId,
                record.Model,
                record.PromptType,
                record.Run
            );
        }

        await BulkCopyAsync(table, _predictionsTableName, cancellationToken);
    }

    public async Task WriteUsageAsync(IEnumerable<UsageRecordWithRun> usageRecords, CancellationToken cancellationToken)
    {
        var records = usageRecords as IReadOnlyCollection<UsageRecordWithRun> ?? usageRecords.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var table = new DataTable();
        table.Columns.Add("Year", typeof(string));
        table.Columns.Add("EssayType", typeof(string));
        table.Columns.Add("BatchIndex", typeof(int));
        table.Columns.Add("Items", typeof(int));
        table.Columns.Add("LatencyMs", typeof(int));
        table.Columns.Add("InputTokens", typeof(int));
        table.Columns.Add("OutputTokens", typeof(int));
        table.Columns.Add("RunDate", typeof(string));
        table.Columns.Add("RunId", typeof(string));
        table.Columns.Add("Model", typeof(string));
        table.Columns.Add("PromptType", typeof(string));
        table.Columns.Add("Run", typeof(string));

        foreach (var record in records)
        {
            table.Rows.Add(
                record.Year,
                record.EssayType,
                record.BatchIndex,
                record.Items,
                record.LatencyMs ?? (object)DBNull.Value,
                record.InputTokens ?? (object)DBNull.Value,
                record.OutputTokens ?? (object)DBNull.Value,
                record.RunDate,
                record.RunId,
                record.Model,
                record.PromptType,
                record.Run
            );
        }

        await BulkCopyAsync(table, _usageTableName, cancellationToken);
    }

    public async Task WriteMetricsAsync(IEnumerable<MetricSummary> metrics, CancellationToken cancellationToken)
    {
        var records = metrics as IReadOnlyCollection<MetricSummary> ?? metrics.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var table = new DataTable();
        table.Columns.Add("RunDate", typeof(string));
        table.Columns.Add("RunId", typeof(string));
        table.Columns.Add("Model", typeof(string));
        table.Columns.Add("PromptType", typeof(string));
        table.Columns.Add("Run", typeof(string));
        table.Columns.Add("Year", typeof(string));
        table.Columns.Add("EssayType", typeof(string));
        table.Columns.Add("count", typeof(int));
        table.Columns.Add("accuracy", typeof(double));
        table.Columns.Add("qwk", typeof(double));
        table.Columns.Add("macro_f1", typeof(double));
        table.Columns.Add("spearman_r", typeof(double));

        foreach (var record in records)
        {
            table.Rows.Add(
                record.RunDate,
                record.RunId,
                record.Model,
                record.PromptType,
                record.Run,
                record.Year,
                record.EssayType,
                record.Count,
                record.Accuracy,
                record.QuadraticWeightedKappa,
                record.MacroF1,
                record.SpearmanR
            );
        }

        await BulkCopyAsync(table, _metricsTableName, cancellationToken);
    }

    private async Task BulkCopyAsync(DataTable table, string destinationTable, CancellationToken cancellationToken)
    {
        await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        using var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = destinationTable
        };

        foreach (DataColumn column in table.Columns)
        {
            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        }

        await bulkCopy.WriteToServerAsync(table, cancellationToken);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
}
