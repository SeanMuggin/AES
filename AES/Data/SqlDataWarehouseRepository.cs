using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Azure.Core;

namespace AES.Evaluator.Data;

public sealed class SqlDataWarehouseRepository : IDataRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly string _rubricsTableName;
    private readonly string _essaysTableName;

    public SqlDataWarehouseRepository(AesEvaluatorOptions.SqlDatabaseOptions options, TokenCredential? credential = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.RubricsTable))
        {
            throw new ArgumentException("Rubrics table name is required.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.EssaysTable))
        {
            throw new ArgumentException("Essays table name is required.", nameof(options));
        }

        _connectionFactory = new SqlConnectionFactory(options.ConnectionString, credential);
        _rubricsTableName = SqlIdentifierHelper.FormatTableName(options.RubricsTable);
        _essaysTableName = SqlIdentifierHelper.FormatTableName(options.EssaysTable);
    }

    public async Task<IReadOnlyList<RubricRecord>> GetRubricsAsync(CancellationToken cancellationToken)
    {
        const string query = "SELECT [Year], [EssayType], [Rubric] FROM {0}";
        return await ExecuteQueryAsync(
            string.Format(CultureInfo.InvariantCulture, query, _rubricsTableName),
            MapRubric,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<EssayRecord>> GetEssaysAsync(CancellationToken cancellationToken)
    {
        const string query = "SELECT [Id], [Year], [EssayType], [EssayContent], [ReaderId], [StudentId], [GoldScore] FROM {0}";
        return await ExecuteQueryAsync(
            string.Format(CultureInfo.InvariantCulture, query, _essaysTableName),
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
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
        var id = SqlIdentifierHelper.ConvertToString(reader["Id"]);
        var year = SqlIdentifierHelper.ConvertToString(reader["Year"]);
        var essayType = SqlIdentifierHelper.ConvertToString(reader["EssayType"]);
        var essayContent = SqlIdentifierHelper.ConvertToString(reader["EssayContent"]);
        var readerId = SqlIdentifierHelper.ConvertToNullableString(reader["ReaderId"]);
        var studentId = SqlIdentifierHelper.ConvertToNullableString(reader["StudentId"]);
        var goldScore = SqlIdentifierHelper.ConvertToNullableInt(reader["GoldScore"]);
        return new EssayRecord(id, year, essayType, essayContent, readerId, studentId, goldScore);
    }
}
