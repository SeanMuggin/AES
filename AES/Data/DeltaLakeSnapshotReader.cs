using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Parquet;

namespace AES.Evaluator.Data;

internal static class DeltaLakeSnapshotReader
{
    internal enum DeltaLogFileType
    {
        Json,
        CheckpointParquet
    }

    internal sealed record DeltaLogFile(string Path, DeltaLogFileType Type, long Version, int Sequence, int TotalParts);

    internal static string CombinePaths(string basePath, string relativePath)
    {
        var normalizedBase = NormalizePath(basePath);
        var normalizedRelative = NormalizePath(relativePath);

        if (string.IsNullOrEmpty(normalizedBase))
        {
            return normalizedRelative;
        }

        if (string.IsNullOrEmpty(normalizedRelative))
        {
            return normalizedBase;
        }

        return string.Create(normalizedBase.Length + 1 + normalizedRelative.Length, (normalizedBase, normalizedRelative), static (span, tuple) =>
        {
            tuple.normalizedBase.AsSpan().CopyTo(span);
            span[tuple.normalizedBase.Length] = '/';
            tuple.normalizedRelative.AsSpan().CopyTo(span[(tuple.normalizedBase.Length + 1)..]);
        });
    }

    internal static bool TryParseDeltaLogFile(string path, out DeltaLogFile file)
    {
        file = null!;
        if (string.IsNullOrWhiteSpace(path))
        {
            return false;
        }

        var fileName = Path.GetFileName(path);

        if (fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            var versionPart = fileName[..^".json".Length];
            if (!long.TryParse(versionPart, NumberStyles.None, CultureInfo.InvariantCulture, out var version))
            {
                return false;
            }

            file = new DeltaLogFile(path, DeltaLogFileType.Json, version, 1, 1);
            return true;
        }

        if (!fileName.Contains(".checkpoint", StringComparison.OrdinalIgnoreCase) || fileName.EndsWith(".crc", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var segments = fileName.Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
        {
            return false;
        }

        if (!long.TryParse(segments[0], NumberStyles.None, CultureInfo.InvariantCulture, out var checkpointVersion))
        {
            return false;
        }

        if (!string.Equals(segments[1], "checkpoint", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var isSinglePart = segments.Length == 3 && string.Equals(segments[2], "parquet", StringComparison.OrdinalIgnoreCase);
        var isMultiPart = segments.Length == 5 && string.Equals(segments[4], "parquet", StringComparison.OrdinalIgnoreCase);

        if (!isSinglePart && !isMultiPart)
        {
            return false;
        }

        var sequence = 1;
        var totalParts = 1;

        if (isMultiPart)
        {
            if (!int.TryParse(segments[2], NumberStyles.None, CultureInfo.InvariantCulture, out sequence) ||
                !int.TryParse(segments[3], NumberStyles.None, CultureInfo.InvariantCulture, out totalParts))
            {
                return false;
            }
        }

        file = new DeltaLogFile(path, DeltaLogFileType.CheckpointParquet, checkpointVersion, sequence, totalParts);
        return true;
    }

    internal static async Task<IReadOnlyList<string>> ReadActiveDataFilesAsync(
        Func<CancellationToken, IAsyncEnumerable<DeltaLogFile>> enumerateLogFiles,
        Func<string, CancellationToken, Task<Stream>> openLogStream,
        string tablePath,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(enumerateLogFiles);
        ArgumentNullException.ThrowIfNull(openLogStream);

        var logFiles = new List<DeltaLogFile>();
        await foreach (var logFile in enumerateLogFiles(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            logFiles.Add(logFile);
        }

        if (logFiles.Count == 0)
        {
            return Array.Empty<string>();
        }

        var latestVersion = logFiles.Where(f => f.Type == DeltaLogFileType.Json).Select(f => (long?)f.Version).Max();
        if (latestVersion is null)
        {
            return Array.Empty<string>();
        }

        var activeFiles = new Dictionary<string, DeltaFileState>(StringComparer.OrdinalIgnoreCase);
        var normalizedTablePath = NormalizePath(tablePath);

        var checkpointGroup = logFiles
            .Where(f => f.Type == DeltaLogFileType.CheckpointParquet && f.Version <= latestVersion)
            .GroupBy(f => f.Version)
            .OrderByDescending(g => g.Key)
            .FirstOrDefault();

        if (checkpointGroup is not null)
        {
            var orderedParts = checkpointGroup
                .OrderBy(part => part.Sequence)
                .ToList();

            foreach (var part in orderedParts)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using var stream = await openLogStream(part.Path, cancellationToken).ConfigureAwait(false);
                await ApplyCheckpointAsync(stream, activeFiles, cancellationToken).ConfigureAwait(false);
            }
        }

        var startingVersion = checkpointGroup?.Key ?? -1;

        var jsonLogs = logFiles
            .Where(f => f.Type == DeltaLogFileType.Json && f.Version > startingVersion && f.Version <= latestVersion)
            .OrderBy(f => f.Version);

        foreach (var log in jsonLogs)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var stream = await openLogStream(log.Path, cancellationToken).ConfigureAwait(false);
            await ApplyJsonLogAsync(stream, activeFiles, cancellationToken).ConfigureAwait(false);
        }

        if (activeFiles.Count == 0)
        {
            return Array.Empty<string>();
        }

        return activeFiles.Values
            .OrderBy(value => value.RelativePath, StringComparer.OrdinalIgnoreCase)
            .Select(value => CombinePaths(normalizedTablePath, value.RelativePath))
            .ToList();
    }

    private static async Task ApplyCheckpointAsync(Stream checkpointStream, IDictionary<string, DeltaFileState> activeFiles, CancellationToken cancellationToken)
    {
        using var parquetReader = await ParquetReader.CreateAsync(checkpointStream, cancellationToken: cancellationToken).ConfigureAwait(false);
        var dataFields = parquetReader.Schema
            .GetDataFields()
            .Where(field => string.Equals(field.Name, "add.path", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(field.Name, "remove.path", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(field => field.Name, field => field, StringComparer.OrdinalIgnoreCase);

        if (dataFields.Count == 0)
        {
            return;
        }

        for (var rowGroupIndex = 0; rowGroupIndex < parquetReader.RowGroupCount; rowGroupIndex++)
        {
            using var groupReader = parquetReader.OpenRowGroupReader(rowGroupIndex);

            Array? addPaths = null;
            Array? removePaths = null;

            if (dataFields.TryGetValue("add.path", out var addField))
            {
                var column = await groupReader.ReadColumnAsync(addField).ConfigureAwait(false);
                addPaths = column.Data;
            }

            if (dataFields.TryGetValue("remove.path", out var removeField))
            {
                var column = await groupReader.ReadColumnAsync(removeField).ConfigureAwait(false);
                removePaths = column.Data;
            }

            var rowCount = Math.Max(addPaths?.Length ?? 0, removePaths?.Length ?? 0);

            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var addPath = GetString(addPaths, rowIndex);
                if (!string.IsNullOrEmpty(addPath))
                {
                    var relativePath = NormalizePath(addPath);
                    if (!string.IsNullOrEmpty(relativePath))
                    {
                        activeFiles[relativePath] = new DeltaFileState(relativePath);
                    }
                }

                var removePath = GetString(removePaths, rowIndex);
                if (!string.IsNullOrEmpty(removePath))
                {
                    var relativePath = NormalizePath(removePath);
                    if (!string.IsNullOrEmpty(relativePath))
                    {
                        activeFiles.Remove(relativePath);
                    }
                }
            }
        }
    }

    private static async Task ApplyJsonLogAsync(Stream logStream, IDictionary<string, DeltaFileState> activeFiles, CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(logStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);

        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync().ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;

            if (root.TryGetProperty("add", out var addElement) && addElement.ValueKind == JsonValueKind.Object)
            {
                if (addElement.TryGetProperty("path", out var pathElement) && pathElement.ValueKind == JsonValueKind.String)
                {
                    var relativePath = NormalizePath(pathElement.GetString() ?? string.Empty);
                    if (!string.IsNullOrEmpty(relativePath))
                    {
                        activeFiles[relativePath] = new DeltaFileState(relativePath);
                    }
                }
            }

            if (root.TryGetProperty("remove", out var removeElement) && removeElement.ValueKind == JsonValueKind.Object)
            {
                if (removeElement.TryGetProperty("path", out var pathElement) && pathElement.ValueKind == JsonValueKind.String)
                {
                    var relativePath = NormalizePath(pathElement.GetString() ?? string.Empty);
                    if (!string.IsNullOrEmpty(relativePath))
                    {
                        activeFiles.Remove(relativePath);
                    }
                }
            }
        }
    }

    private static string? GetString(Array? values, int index)
    {
        if (values is null || index < 0 || index >= values.Length)
        {
            return null;
        }

        return values.GetValue(index) switch
        {
            null => null,
            string stringValue => stringValue,
            _ => Convert.ToString(values.GetValue(index), CultureInfo.InvariantCulture)
        };
    }

    private static string NormalizePath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return string.Empty;
        }

        var normalized = path.Replace('\\', '/');
        return normalized.Trim('/');
    }

    private sealed record DeltaFileState(string RelativePath);
}
