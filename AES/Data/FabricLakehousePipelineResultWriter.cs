using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;

namespace AES.Evaluator.Data;

internal interface ILakehouseClientFactory
{
    ILakehouseFileSystemClient Create(Uri fileSystemUri, TokenCredential credential);
}

internal interface ILakehouseFileSystemClient
{
    ILakehouseDirectoryClient GetDirectoryClient(string directoryPath);
}

internal interface ILakehouseDirectoryClient
{
    Task CreateIfNotExistsAsync(CancellationToken cancellationToken);

    ILakehouseFileClient GetFileClient(string fileName);
}

internal interface ILakehouseFileClient
{
    Task UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken);
}

public sealed class FabricLakehousePipelineResultWriter : IPipelineResultWriter, IDisposable
{
    private const string FilesPrefix = "Files";
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly string _workspaceId;
    private readonly string _lakehouseId;
    private readonly string _predictionsTableName;
    private readonly string _usageTableName;
    private readonly string _metricsTableName;
    private readonly Uri _fileSystemUri;
    private readonly TokenCredential _credential;
    private readonly HttpClient _httpClient;
    private readonly ILakehouseClientFactory _fileSystemClientFactory;

    public FabricLakehousePipelineResultWriter(
        AesEvaluatorOptions.DatabaseOptions options,
        HttpClient? httpClient = null,
        TokenCredential? credential = null,
        ILakehouseClientFactory? clientFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.RubricsTableEndpoint))
        {
            throw new ArgumentException(
                "Rubrics table endpoint is required to determine Fabric workspace metadata.",
                nameof(options));
        }

        (_workspaceId, _lakehouseId) = ParseWorkspaceAndLakehouse(options.RubricsTableEndpoint);

        _predictionsTableName = options.PredictionsTable;
        _usageTableName = options.UsageTable;
        _metricsTableName = options.MetricsByRubricTable;

        _fileSystemUri = BuildFileSystemUri(_workspaceId, _lakehouseId);
        _credential = credential ?? new DefaultAzureCredential();
        _httpClient = httpClient ?? new HttpClient();
        _fileSystemClientFactory = clientFactory ?? new DataLakeClientFactory();
    }

    public async Task WritePredictionsAsync(
        IEnumerable<ScoredEssayRecord> predictions,
        CancellationToken cancellationToken)
    {
        var records = predictions as IReadOnlyCollection<ScoredEssayRecord> ?? predictions.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var headers = new[]
        {
            "Id",
            "Year",
            "EssayType",
            "ReaderId",
            "StudentId",
            "GoldScore",
            "PredScore",
            "PredRationale",
            "RunDate",
            "RunId",
            "Model",
            "PromptType",
            "Run"
        };

        await UploadAndLoadAsync(
            records,
            headers,
            r => new object?[]
            {
                r.Id,
                r.Year,
                r.EssayType,
                r.ReaderId,
                r.StudentId,
                r.GoldScore,
                r.PredScore,
                r.PredRationale,
                r.RunDate,
                r.RunId,
                r.Model,
                r.PromptType,
                r.Run
            },
            _predictionsTableName,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task WriteUsageAsync(
        IEnumerable<UsageRecordWithRun> usageRecords,
        CancellationToken cancellationToken)
    {
        var records = usageRecords as IReadOnlyCollection<UsageRecordWithRun> ?? usageRecords.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var headers = new[]
        {
            "Year",
            "EssayType",
            "BatchIndex",
            "Items",
            "LatencyMs",
            "InputTokens",
            "OutputTokens",
            "RunDate",
            "RunId",
            "Model",
            "PromptType",
            "Run"
        };

        await UploadAndLoadAsync(
            records,
            headers,
            r => new object?[]
            {
                r.Year,
                r.EssayType,
                r.BatchIndex,
                r.Items,
                r.LatencyMs,
                r.InputTokens,
                r.OutputTokens,
                r.RunDate,
                r.RunId,
                r.Model,
                r.PromptType,
                r.Run
            },
            _usageTableName,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task WriteMetricsAsync(
        IEnumerable<MetricSummary> metrics,
        CancellationToken cancellationToken)
    {
        var records = metrics as IReadOnlyCollection<MetricSummary> ?? metrics.ToList();
        if (records.Count == 0)
        {
            return;
        }

        var headers = new[]
        {
            "RunDate",
            "RunId",
            "Model",
            "PromptType",
            "Run",
            "Year",
            "EssayType",
            "count",
            "accuracy",
            "qwk",
            "macro_f1",
            "spearman_r"
        };

        await UploadAndLoadAsync(
            records,
            headers,
            r => new object?[]
            {
                r.RunDate,
                r.RunId,
                r.Model,
                r.PromptType,
                r.Run,
                r.Year,
                r.EssayType,
                r.Count,
                r.Accuracy,
                r.QuadraticWeightedKappa,
                r.MacroF1,
                r.SpearmanR
            },
            _metricsTableName,
            cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        _httpClient.Dispose();
        GC.SuppressFinalize(this);
    }

    private async Task UploadAndLoadAsync<T>(
        IReadOnlyCollection<T> records,
        IReadOnlyList<string> headers,
        Func<T, object?[]> valueSelector,
        string tableName,
        CancellationToken cancellationToken)
    {
        var csv = BuildCsv(records, headers, valueSelector);
        var relativePath = BuildRelativePath(tableName);
        await UploadToLakehouseAsync(relativePath, csv, cancellationToken).ConfigureAwait(false);
        await InvokeLoadToTableAsync(tableName, relativePath, cancellationToken).ConfigureAwait(false);
    }

    private static string BuildCsv<T>(
        IEnumerable<T> records,
        IReadOnlyList<string> headers,
        Func<T, object?[]> valueSelector)
    {
        var builder = new StringBuilder();
        builder.AppendLine(string.Join(',', headers.Select(EscapeCsvValue)));

        foreach (var record in records)
        {
            var values = valueSelector(record);
            builder.AppendLine(string.Join(',', values.Select(EscapeCsvValue)));
        }

        return builder.ToString();
    }

    private async Task UploadToLakehouseAsync(
        string relativePath,
        string csvContent,
        CancellationToken cancellationToken)
    {
        var fileSystemClient = _fileSystemClientFactory.Create(_fileSystemUri, _credential);
        var segments = relativePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 2)
        {
            throw new InvalidOperationException(
                $"Relative path '{relativePath}' must include at least one directory (e.g. 'Files/filename.csv').");
        }

        var fileName = segments[^1];
        var directoryPath = string.Join('/', segments.Take(segments.Length - 1));
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
        try
        {
            await directoryClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch(Exception e)
        {
            string message = e.Message;
        }
        var fileClient = directoryClient.GetFileClient(fileName);

        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        await fileClient.UploadAsync(stream, overwrite: true, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private async Task InvokeLoadToTableAsync(
        string tableName,
        string relativePath,
        CancellationToken cancellationToken)
    {
        var requestUri = new Uri(
            $"https://api.fabric.microsoft.com/v1/workspaces/{_workspaceId}/lakehouses/{_lakehouseId}/tables/{tableName}/load");

        using var request = new HttpRequestMessage(HttpMethod.Post, requestUri)
        {
            Content = new StringContent(
                JsonSerializer.Serialize(new
                {
                    relativePath,
                    pathType = "File",
                    mode = "append",
                    formatOptions = new
                    {
                        header = "true",
                        delimiter = ",",
                        format = "CSV"
                    }
                }, SerializerOptions),
                Encoding.UTF8,
                "application/json")
        };

        await AuthorizeRequestAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.Accepted)
        {
            var operationLocation = response.Headers.Location;
            if (operationLocation is null)
            {
                throw new InvalidOperationException(
                    "Fabric load to table API returned 202 Accepted without an operation location header.");
            }

            await PollOperationAsync(operationLocation, cancellationToken).ConfigureAwait(false);
            return;
        }

        response.EnsureSuccessStatusCode();
    }

    private async Task PollOperationAsync(Uri operationUri, CancellationToken cancellationToken)
    {
        while (true)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, operationUri);
            await AuthorizeRequestAsync(request, cancellationToken).ConfigureAwait(false);
            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);

            if (!document.RootElement.TryGetProperty("status", out var statusProperty))
            {
                throw new InvalidOperationException(
                    "Fabric operation status response did not include a status property.");
            }

            var status = statusProperty.GetString();
            if (string.Equals(status, "Succeeded", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (string.Equals(status, "Failed", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(status, "Canceled", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Fabric load to table operation {status?.ToLowerInvariant()}.");
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task AuthorizeRequestAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var tokenContext = new TokenRequestContext(new[] { "https://analysis.windows.net/powerbi/api/.default" });
        var token = await _credential.GetTokenAsync(tokenContext, cancellationToken).ConfigureAwait(false);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
    }

    private static string BuildRelativePath(string tableName)
    {
        var fileName = $"{tableName}_{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}.csv";
        return string.Join('/', new[] { FilesPrefix, "aes", tableName, fileName });
    }

    private static string EscapeCsvValue(object? value)
    {
        if (value is null)
        {
            return string.Empty;
        }

        var stringValue = value switch
        {
            string str => str,
            IFormattable formattable =>
                formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty,
            _ => value.ToString() ?? string.Empty
        };

        var needsQuotes = stringValue.Contains('"') ||
                          stringValue.Contains(',') ||
                          stringValue.Contains('\n') ||
                          stringValue.Contains('\r');
        if (needsQuotes)
        {
            var escaped = stringValue.Replace("\"", "\"\"");
            return $"\"{escaped}\"";
        }

        return stringValue;
    }

    private static (string WorkspaceId, string LakehouseId) ParseWorkspaceAndLakehouse(string tableEndpoint)
    {
        if (!Uri.TryCreate(tableEndpoint, UriKind.Absolute, out var uri))
        {
            throw new ArgumentException(
                "The Fabric table endpoint is not a valid absolute URI.",
                nameof(tableEndpoint));
        }

        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 2)
        {
            throw new ArgumentException(
                "The Fabric table endpoint did not include workspace and lakehouse identifiers.",
                nameof(tableEndpoint));
        }

        var workspaceId = segments[0];
        var lakehouseId = segments[1];
        if (lakehouseId.EndsWith(".Lakehouse", StringComparison.OrdinalIgnoreCase))
        {
            lakehouseId = lakehouseId[..^".Lakehouse".Length];
        }

        return (workspaceId, lakehouseId);
    }

    private static Uri BuildFileSystemUri(string workspaceId, string lakehouseId)
    {
        var lakehouseSegment = lakehouseId.EndsWith(".Lakehouse", StringComparison.OrdinalIgnoreCase)
            ? lakehouseId
            : lakehouseId + ".Lakehouse";

        return new Uri($"https://onelake.dfs.fabric.microsoft.com/{workspaceId}/{lakehouseSegment}");
    }

    private sealed class DataLakeClientFactory : ILakehouseClientFactory
    {
        public ILakehouseFileSystemClient Create(Uri fileSystemUri, TokenCredential credential)
        {
            var client = new DataLakeFileSystemClient(fileSystemUri, credential);
            return new DataLakeFileSystemClientAdapter(client);
        }

        private sealed class DataLakeFileSystemClientAdapter : ILakehouseFileSystemClient
        {
            private readonly DataLakeFileSystemClient _client;

            public DataLakeFileSystemClientAdapter(DataLakeFileSystemClient client)
            {
                _client = client;
            }

            public ILakehouseDirectoryClient GetDirectoryClient(string directoryPath)
            {
                var directoryClient = _client.GetDirectoryClient(directoryPath);
                return new DataLakeDirectoryClientAdapter(directoryClient);
            }
        }

        private sealed class DataLakeDirectoryClientAdapter : ILakehouseDirectoryClient
        {
            private readonly DataLakeDirectoryClient _client;

            public DataLakeDirectoryClientAdapter(DataLakeDirectoryClient client)
            {
                _client = client;
            }

            public async Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
            {
                await _client.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            public ILakehouseFileClient GetFileClient(string fileName)
            {
                var fileClient = _client.GetFileClient(fileName);
                return new DataLakeFileClientAdapter(fileClient);
            }
        }

        private sealed class DataLakeFileClientAdapter : ILakehouseFileClient
        {
            private readonly DataLakeFileClient _client;

            public DataLakeFileClientAdapter(DataLakeFileClient client)
            {
                _client = client;
            }

            public async Task UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken)
            {
                await _client.UploadAsync(content, overwrite, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
