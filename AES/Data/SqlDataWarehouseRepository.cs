using System;
using System.Collections.Generic;
using System.Data.Common;
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
        _rubricsTableName = options.RubricsTable;
        _essaysTableName = options.EssaysTable;
    }

    public async Task<IReadOnlyList<RubricRecord>> GetRubricsAsync(CancellationToken cancellationToken)
    {
        string query = $"WITH rubric_scores AS (\r\n    SELECT\r\n        appyear,\r\n        essay,\r\n        CONCAT(\r\n            'Score ',\r\n            score,\r\n            ': ',\r\n            STRING_AGG(\r\n                COALESCE(STRING_ESCAPE(score_criteria, 'json'), N''),\r\n                '; '\r\n            ) WITHIN GROUP (ORDER BY score_criteria)\r\n        ) AS ScoreLabel\r\n    FROM LH_DSP_AES.dbo.dsp_rubric\r\n    GROUP BY appyear, essay, score\r\n),\r\nrubric_summary AS (\r\n    SELECT\r\n        appyear AS [Year],\r\n        essay AS EssayType,\r\n        STRING_AGG(ScoreLabel, '. ') AS Rubric\r\n    FROM rubric_scores\r\n    GROUP BY appyear, essay\r\n),\r\nessay_questions AS (\r\n    SELECT\r\n        [Year],\r\n        EssayType,\r\n        CONCAT('Essay Question: ', Prompt) AS EssayQuestion\r\n    FROM [LH_DSP_AES].[dbo].[essay_questions]\r\n)\r\nSELECT\r\n    rs.[Year],\r\n    rs.EssayType,\r\n    CONCAT(eq.EssayQuestion, ' ', rs.Rubric) AS Rubric\r\nFROM rubric_summary rs\r\nJOIN essay_questions eq\r\n    ON rs.[Year] = eq.[Year]\r\n   AND rs.EssayType = eq.EssayType;";
        return await ExecuteQueryAsync(
            query,
            MapRubric,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<EssayRecord>> GetEssaysAsync(CancellationToken cancellationToken)
    {
        string query = $"SELECT \r\n      ROW_NUMBER() OVER (ORDER BY ar.Year ASC) AS Id\r\n      ,[Year]\r\n      ,[EssayType]\r\n      ,[EssayContent]\r\n      ,ar.ReaderId\r\n      ,StudentId\r\n      ,Score GoldScore\r\n  FROM [dbo].[tblDSP_ApplicantReaderDetails] ar\r\n  join [dbo].[App_tblReader] r\r\n    on ar.ReaderId = r.ReaderId\r\n    and r.IsSuperReader = 1\r\n  where \r\n    EssayType in ('DegreeFit','CollegeChoice')\r\n    and [Year] in (2023,2024,2025)\r\n    and ReaderScope = 'FinalSelection'";

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
