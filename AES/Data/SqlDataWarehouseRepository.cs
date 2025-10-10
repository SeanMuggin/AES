using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Azure.Core;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;

namespace AES.Evaluator.Data;

public sealed class SqlDataWarehouseRepository : IDataRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly string _rubricQueryPath;
    private readonly string _essayQueryPath;

    public SqlDataWarehouseRepository(AesEvaluatorOptions options, TokenCredential? credential = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.SqlDatabase);

        var dbOptions = options.SqlDatabase;
        if (string.IsNullOrWhiteSpace(dbOptions.ConnectionString))
        {
            throw new ArgumentException("Connection string is required.", nameof(options));
        }

        _connectionFactory = new SqlConnectionFactory(dbOptions.ConnectionString, credential);

        var (rubricFile, essayFile) = options.Mode switch
        {
            AesEvaluatorOptions.EvaluatorMode.ModelTesting => ("rubric_ModelTesting.sql", "essays_ModelTesting.sql"),
            AesEvaluatorOptions.EvaluatorMode.Aes => ("rubric_AES.sql", "essays_AES.sql"),
            _ => throw new ArgumentOutOfRangeException(nameof(options), $"Unsupported evaluator mode: {options.Mode}.")
        };

        var basePath = AppContext.BaseDirectory;
        _rubricQueryPath = Path.Combine(basePath, "SqlQueries", rubricFile);
        _essayQueryPath = Path.Combine(basePath, "SqlQueries", essayFile);

        if (!File.Exists(_rubricQueryPath))
        {
            throw new FileNotFoundException($"Rubric query file not found: {_rubricQueryPath}");
        }

        if (!File.Exists(_essayQueryPath))
        {
            throw new FileNotFoundException($"Essay query file not found: {_essayQueryPath}");
        }
    }

    public async Task<IReadOnlyList<RubricRecord>> GetRubricsAsync(CancellationToken cancellationToken)
    {
        var query = await File.ReadAllTextAsync(_rubricQueryPath, cancellationToken).ConfigureAwait(false);

        return await ExecuteQueryAsync(
            query,
            MapRubric,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<EssayRecord>> GetEssaysAsync(CancellationToken cancellationToken)
    {
        var query = await File.ReadAllTextAsync(_essayQueryPath, cancellationToken).ConfigureAwait(false);

        return await ExecuteQueryAsync(
            query,
            MapEssay,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<T>> ExecuteQueryAsync<T>(
        string commandText,
        Func<DbDataReader, T> materializer,
        CancellationToken cancellationToken)
    {
        var results = new List<T>();

        await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            results.Add(materializer(reader));
        }

        return results;
    }

    private static RubricRecord MapRubric(DbDataReader reader)
    {
        var year = SqlIdentifierHelper.ConvertToString(reader["Year"]);
        var essayType = SqlIdentifierHelper.ConvertToString(reader["EssayType"]);
        var rubric = SqlIdentifierHelper.ConvertToString(reader["Rubric"]);
        return new RubricRecord(year, essayType, rubric);
    }

    private static EssayRecord MapEssay(DbDataReader reader)
    {
        var id = SqlIdentifierHelper.ConvertToString(reader["EssayId"]);
        var year = SqlIdentifierHelper.ConvertToString(reader["Year"]);
        var essayType = SqlIdentifierHelper.ConvertToString(reader["EssayType"]);
        var essayContent = SqlIdentifierHelper.ConvertToString(reader["EssayContent"]);
        var readerId = SqlIdentifierHelper.ConvertToNullableString(reader["ReaderId"]);
        var studentId = SqlIdentifierHelper.ConvertToNullableString(reader["StudentId"]);
        var goldScore = SqlIdentifierHelper.ConvertToNullableInt(reader["GoldScore"]);
        return new EssayRecord(id, year, essayType, essayContent, readerId, studentId, goldScore);
    }
}

