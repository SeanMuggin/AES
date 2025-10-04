using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Specialized;
using Parquet;

namespace AES.Evaluator.Data;

public sealed class FabricLakehouseDataRepository : IDataRepository
{
    private readonly Uri _rubricsUri;
    private readonly Uri _essaysUri;
    private readonly TokenCredential _credential;

    public FabricLakehouseDataRepository(AesEvaluatorOptions.DatabaseOptions options, TokenCredential? credential = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.RubricsTableEndpoint))
        {
            throw new ArgumentException("Rubrics table endpoint is required.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.EssaysTableEndpoint))
        {
            throw new ArgumentException("Essays table endpoint is required.", nameof(options));
        }

        _rubricsUri = new Uri(options.RubricsTableEndpoint, UriKind.Absolute);
        _essaysUri = new Uri(options.EssaysTableEndpoint, UriKind.Absolute);
        _credential = credential ?? new DefaultAzureCredential();
    }

    public Task<IReadOnlyList<RubricRecord>> GetRubricsAsync(CancellationToken cancellationToken)
        => ReadTableAsync(_rubricsUri, MapRubric, cancellationToken);

    public Task<IReadOnlyList<EssayRecord>> GetEssaysAsync(CancellationToken cancellationToken)
        => ReadTableAsync(_essaysUri, MapEssay, cancellationToken);

    private async Task<IReadOnlyList<T>> ReadTableAsync<T>(Uri tableUri, Func<Dictionary<string, object?>, T> materializer, CancellationToken cancellationToken)
    {
        var directoryClient = new DataLakeDirectoryClient(tableUri, _credential);
        var fileSystemClient = directoryClient.GetParentFileSystemClient();

        var tablePath = directoryClient.Path;
        var deltaLogPath = DeltaLakeSnapshotReader.CombinePaths(tablePath, "_delta_log");

        static async IAsyncEnumerable<DeltaLakeSnapshotReader.DeltaLogFile> EnumerateDeltaLogFilesAsync(
            DataLakeFileSystemClient fileSystemClient,
            string deltaLogPath,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var pathItem in fileSystemClient.GetPathsAsync(deltaLogPath, recursive: false, cancellationToken: cancellationToken))
            {
                if (pathItem.IsDirectory == true)
                {
                    continue;
                }

                if (DeltaLakeSnapshotReader.TryParseDeltaLogFile(pathItem.Name, out var logFile))
                {
                    yield return logFile;
                }
            }
        }

        async Task<Stream> OpenLogStreamAsync(string path, CancellationToken token)
        {
            var fileClient = fileSystemClient.GetFileClient(path);
            var stream = new MemoryStream();
            await fileClient.ReadToAsync(stream, cancellationToken: token).ConfigureAwait(false);
            stream.Position = 0;
            return stream;
        }

        var activeDataFiles = await DeltaLakeSnapshotReader.ReadActiveDataFilesAsync(
            cancellationToken => EnumerateDeltaLogFilesAsync(fileSystemClient, deltaLogPath, cancellationToken),
            OpenLogStreamAsync,
            tablePath,
            cancellationToken).ConfigureAwait(false);

        var results = new List<T>();

        foreach (var dataFilePath in activeDataFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileClient = fileSystemClient.GetFileClient(dataFilePath);
            await using var stream = new MemoryStream();
            await fileClient.ReadToAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            stream.Position = 0;

            using var parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            var dataFields = parquetReader.Schema.GetDataFields();

            for (var rowGroup = 0; rowGroup < parquetReader.RowGroupCount; rowGroup++)
            {
                using var groupReader = parquetReader.OpenRowGroupReader(rowGroup);
                var columns = new Dictionary<string, Array>(StringComparer.OrdinalIgnoreCase);

                foreach (var field in dataFields)
                {
                    var column = await groupReader.ReadColumnAsync(field).ConfigureAwait(false);
                    columns[field.Name] = column.Data;
                }

                if (columns.Count == 0)
                {
                    continue;
                }

                var rowCount = columns.Values.Max(arr => arr.Length);

                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    foreach (var kvp in columns)
                    {
                        row[kvp.Key] = rowIndex < kvp.Value.Length ? GetArrayValue(kvp.Value, rowIndex) : null;
                    }

                    results.Add(materializer(row));
                }
            }
        }

        return results;
    }

    private static object? GetArrayValue(Array array, int index)
    {
        var value = array.GetValue(index);
        if (value is DateTime dateTime)
        {
            return dateTime.ToString("o", CultureInfo.InvariantCulture);
        }

        if (value is DateOnly dateOnly)
        {
            return dateOnly.ToString("O", CultureInfo.InvariantCulture);
        }

        return value;
    }

    private static RubricRecord MapRubric(Dictionary<string, object?> row)
    {
        return new RubricRecord(
            Year: Convert.ToString(row.GetValueOrDefault("Year"), CultureInfo.InvariantCulture) ?? string.Empty,
            EssayType: Convert.ToString(row.GetValueOrDefault("EssayType"), CultureInfo.InvariantCulture) ?? string.Empty,
            Rubric: Convert.ToString(row.GetValueOrDefault("Rubric"), CultureInfo.InvariantCulture) ?? string.Empty
        );
    }

    private static EssayRecord MapEssay(Dictionary<string, object?> row)
    {
        return new EssayRecord(
            Id: Convert.ToString(row.GetValueOrDefault("Id"), CultureInfo.InvariantCulture) ?? string.Empty,
            Year: Convert.ToString(row.GetValueOrDefault("Year"), CultureInfo.InvariantCulture) ?? string.Empty,
            EssayType: Convert.ToString(row.GetValueOrDefault("EssayType"), CultureInfo.InvariantCulture) ?? string.Empty,
            EssayContent: Convert.ToString(row.GetValueOrDefault("EssayContent"), CultureInfo.InvariantCulture) ?? string.Empty,
            ReaderId: ConvertToNullableString(row, "ReaderId"),
            StudentId: ConvertToNullableString(row, "StudentId"),
            GoldScore: ConvertToNullableInt(row, "GoldScore")
        );
    }

    private static string? ConvertToNullableString(IDictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var value))
        {
            return null;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture);
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private static int? ConvertToNullableInt(IDictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }

        return value switch
        {
            int intValue => intValue,
            long longValue => (int)longValue,
            short shortValue => shortValue,
            byte byteValue => byteValue,
            sbyte sbyteValue => sbyteValue,
            double doubleValue => Convert.ToInt32(Math.Round(doubleValue, MidpointRounding.AwayFromZero)),
            float floatValue => Convert.ToInt32(Math.Round(floatValue, MidpointRounding.AwayFromZero)),
            decimal decimalValue => Convert.ToInt32(Math.Round(decimalValue, MidpointRounding.AwayFromZero)),
            string stringValue when int.TryParse(stringValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => null
        };
    }
}
