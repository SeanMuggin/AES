using System.Data;
using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Azure.Identity;
using Microsoft.Data.SqlClient;

namespace AES.Evaluator.Data;

public sealed class SqlPipelineResultWriter : IPipelineResultWriter
{
    private readonly AesEvaluatorOptions.DatabaseOptions _options;

    public SqlPipelineResultWriter(AesEvaluatorOptions.DatabaseOptions options)
    {
        _options = options;
    }

    public async Task WritePredictionsAsync(IEnumerable<ScoredEssayRecord> predictions, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new InvalidOperationException("Connection string is required to persist predictions.");
        }

        var table = new DataTable();
        table.Columns.Add("Id", typeof(string));
        table.Columns.Add("Year", typeof(string));
        table.Columns.Add("EssayType", typeof(string));
        table.Columns.Add("ReaderId", typeof(string));
        table.Columns.Add("StudentId", typeof(string));
        table.Columns.Add("GoldScore", typeof(int));
        table.Columns.Add("PredScore", typeof(int));
        table.Columns.Add("PredRationale", typeof(string));
        table.Columns.Add("RunDate", typeof(string));
        table.Columns.Add("RunId", typeof(string));
        table.Columns.Add("Model", typeof(string));
        table.Columns.Add("PromptType", typeof(string));
        table.Columns.Add("Run", typeof(string));

        foreach (var record in predictions)
        {
            table.Rows.Add(
                record.Id,
                record.Year,
                record.EssayType,
                record.ReaderId ?? (object)DBNull.Value,
                record.StudentId ?? (object)DBNull.Value,
                record.GoldScore ?? (object)DBNull.Value,
                record.PredScore ?? (object)DBNull.Value,
                record.PredRationale ?? string.Empty,
                record.RunDate,
                record.RunId,
                record.Model,
                record.PromptType,
                record.Run
            );
        }

        await BulkCopyAsync(table, _options.PredictionsTable, cancellationToken);
    }

    public async Task WriteUsageAsync(IEnumerable<UsageRecordWithRun> usageRecords, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new InvalidOperationException("Connection string is required to persist usage records.");
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

        foreach (var record in usageRecords)
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

        await BulkCopyAsync(table, _options.UsageTable, cancellationToken);
    }

    public async Task WriteMetricsAsync(IEnumerable<MetricSummary> metrics, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new InvalidOperationException("Connection string is required to persist metrics.");
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

        foreach (var record in metrics)
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

        await BulkCopyAsync(table, _options.MetricsByRubricTable, cancellationToken);
    }

    private async Task BulkCopyAsync(DataTable table, string destinationTable, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(cancellationToken);
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
}
