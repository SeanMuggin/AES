using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Azure.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace AES.Evaluator.Data;

public sealed class SqlDataWarehouseRepository : IDataRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly string _rubricsTableName;
    private readonly string _essaysTableName;
    private readonly bool _includeGoldScore;

    public SqlDataWarehouseRepository(AesEvaluatorOptions.SqlDatabaseOptions options, TokenCredential? credential = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        if(string.IsNullOrWhiteSpace(options.RubricsTable))
        {
            throw new ArgumentException("Rubrics table name is required.", nameof(options));
        }

        if(string.IsNullOrWhiteSpace(options.EssaysTable))
        {
            throw new ArgumentException("Essays table name is required.", nameof(options));
        }

        _connectionFactory = new SqlConnectionFactory(options.ConnectionString, credential);
        _rubricsTableName = options.RubricsTable;
        _essaysTableName = options.EssaysTable;
        _includeGoldScore = options.IncludeGoldScore;
    }

    public async Task<IReadOnlyList<RubricRecord>> GetRubricsAsync(CancellationToken cancellationToken)
    {
        string query = File.ReadAllText("SqlQueries/rubric_ModelTesting.sql");  // for ModelTesting Mode
        //string query = File.ReadAllText("SqlQueries/rubric_AES.sql");    // for AES mode

        return await ExecuteQueryAsync(
            query,
            MapRubric,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<EssayRecord>> GetEssaysAsync(CancellationToken cancellationToken)
    {
        string query = File.ReadAllText("SqlQueries/essays_ModelTesting.sql");   // for ModelTesting Mode
        //string query = File.ReadAllText("SqlQueries/essays_AES.sql");    // for AES mode


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

    private EssayRecord MapEssay(DbDataReader reader)
    {
        var id = SqlIdentifierHelper.ConvertToString(reader["EssayId"]);
        var year = SqlIdentifierHelper.ConvertToString(reader["Year"]);
        var essayType = SqlIdentifierHelper.ConvertToString(reader["EssayType"]);
        var essayContent = SqlIdentifierHelper.ConvertToString(reader["EssayContent"]);
        var readerId = SqlIdentifierHelper.ConvertToNullableString(reader["ReaderId"]);
        var studentId = SqlIdentifierHelper.ConvertToNullableString(reader["StudentId"]);
        int? goldScore = null;
        if (_includeGoldScore && ColumnExists(reader, "GoldScore"))
        {
            goldScore = SqlIdentifierHelper.ConvertToNullableInt(reader["GoldScore"]);
        }
        return new EssayRecord(id, year, essayType, essayContent, readerId, studentId, goldScore);
    }

    private static bool ColumnExists(DbDataReader reader, string columnName)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (string.Equals(reader.GetName(i), columnName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}


