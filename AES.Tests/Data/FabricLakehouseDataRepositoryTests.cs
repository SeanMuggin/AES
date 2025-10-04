using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AES.Evaluator.Data;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace AES.Tests.Data;

public class FabricLakehouseDataRepositoryTests
{
    [Fact]
    public async Task ReadActiveFilesFromDeltaLog_SkipsRetiredParquetFiles()
    {
        const string tablePath = "tables/mytable";
        const string retiredFile = "part-00000-1111.snappy.parquet";
        const string currentFile = "part-00001-2222.snappy.parquet";

        var logFilePaths = new[]
        {
            $"{tablePath}/_delta_log/00000000000000000000.json",
            $"{tablePath}/_delta_log/00000000000000000001.json",
            $"{tablePath}/_delta_log/00000000000000000002.json"
        };

        var logFiles = logFilePaths
            .Select(path =>
            {
                Assert.True(DeltaLakeSnapshotReader.TryParseDeltaLogFile(path, out var logFile));
                return logFile;
            })
            .ToList();

        var logContents = new Dictionary<string, string>
        {
            [logFilePaths[0]] = $"{{\"add\":{{\"path\":\"{retiredFile}\",\"size\":100,\"modificationTime\":1,\"dataChange\":true}}}}",
            [logFilePaths[1]] = $"{{\"remove\":{{\"path\":\"{retiredFile}\",\"deletionTimestamp\":2,\"dataChange\":true}}}}",
            [logFilePaths[2]] = $"{{\"add\":{{\"path\":\"{currentFile}\",\"size\":200,\"modificationTime\":3,\"dataChange\":true}}}}"
        };

        async IAsyncEnumerable<DeltaLakeSnapshotReader.DeltaLogFile> EnumerateLogs(CancellationToken cancellationToken)
        {
            foreach (var logFile in logFiles)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return logFile;
                await Task.Yield();
            }
        }

        Task<Stream> OpenLogAsync(string path, CancellationToken cancellationToken)
        {
            var payload = Encoding.UTF8.GetBytes(logContents[path]);
            Stream stream = new MemoryStream(payload, writable: false);
            return Task.FromResult(stream);
        }

        var activeFiles = await DeltaLakeSnapshotReader.ReadActiveDataFilesAsync(
            EnumerateLogs,
            OpenLogAsync,
            tablePath,
            CancellationToken.None);

        var retiredFullPath = DeltaLakeSnapshotReader.CombinePaths(tablePath, retiredFile);
        var currentFullPath = DeltaLakeSnapshotReader.CombinePaths(tablePath, currentFile);

        Assert.Single(activeFiles);
        Assert.Equal(currentFullPath, activeFiles[0]);

        var dataFiles = new Dictionary<string, byte[]>
        {
            [retiredFullPath] = CreateParquetBytes(new Dictionary<string, object?>
            {
                ["Id"] = new[] { "1" },
                ["Value"] = new[] { "retired" }
            }),
            [currentFullPath] = CreateParquetBytes(new Dictionary<string, object?>
            {
                ["Id"] = new[] { "1" },
                ["Value"] = new[] { "current" }
            })
        };

        var rows = new List<Dictionary<string, object?>>();
        foreach (var file in activeFiles)
        {
            using var stream = new MemoryStream(dataFiles[file], writable: false);
            using var reader = await ParquetReader.CreateAsync(stream);
            var dataFields = reader.Schema.GetDataFields();

            for (var groupIndex = 0; groupIndex < reader.RowGroupCount; groupIndex++)
            {
                using var rowGroup = reader.OpenRowGroupReader(groupIndex);
                var columns = new Dictionary<string, Array>(StringComparer.OrdinalIgnoreCase);

                foreach (var field in dataFields)
                {
                    var column = await rowGroup.ReadColumnAsync(field);
                    columns[field.Name] = column.Data;
                }

                var rowCount = columns.Values.Max(array => array.Length);
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    foreach (var column in columns)
                    {
                        row[column.Key] = rowIndex < column.Value.Length ? column.Value.GetValue(rowIndex) : null;
                    }

                    rows.Add(row);
                }
            }
        }

        var result = Assert.Single(rows);
        Assert.Equal("1", Assert.IsType<string>(result["Id"]));
        Assert.Equal("current", Assert.IsType<string>(result["Value"]));
        Assert.DoesNotContain(rows, r => string.Equals("retired", r.GetValueOrDefault("Value") as string, StringComparison.Ordinal));
    }

    private static byte[] CreateParquetBytes(Dictionary<string, object?> columns)
    {
        var dataFields = columns
            .Select(column => column.Value switch
            {
                string[] => new DataField<string>(column.Key),
                int[] => new DataField<int>(column.Key),
                _ => new DataField<string>(column.Key)
            })
            .ToArray();

        var schema = new Schema(dataFields);
        using var stream = new MemoryStream();
        using (var writer = new ParquetWriter(schema, stream))
        {
            using var rowGroup = writer.CreateRowGroup();
            foreach (var field in dataFields)
            {
                if (!columns.TryGetValue(field.Name, out var values) || values is not Array arrayValues)
                {
                    continue;
                }

                var dataColumn = new DataColumn(field, arrayValues);
                rowGroup.WriteColumn(dataColumn);
            }
        }

        return stream.ToArray();
    }
}
