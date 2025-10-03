using System.Data;
using AES.Evaluator.Configuration;
using AES.Evaluator.Models;
using Dapper;

namespace AES.Evaluator.Data;

public sealed class SqlDataRepository : IDataRepository
{
    private readonly AesEvaluatorOptions.DatabaseOptions _options;
    private readonly Func<IDbConnection> _connectionFactory;

    public SqlDataRepository(AesEvaluatorOptions.DatabaseOptions options, Func<IDbConnection> connectionFactory)
    {
        _options = options;
        _connectionFactory = connectionFactory;
    }

    public async Task<IReadOnlyList<RubricRecord>> GetRubricsAsync(CancellationToken cancellationToken)
    {
        var sql = $"SELECT Year, EssayType, Rubric FROM {_options.RubricsTable}";
        var connection = _connectionFactory();
        if (connection is not System.Data.Common.DbConnection dbConnection)
        {
            connection.Dispose();
            throw new InvalidOperationException("Connection factory must return a DbConnection instance.");
        }

        await using (dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken);
            var result = await dbConnection.QueryAsync<RubricRecord>(new CommandDefinition(sql, cancellationToken: cancellationToken));
            return result.AsList();
        }
    }

    public async Task<IReadOnlyList<EssayRecord>> GetEssaysAsync(CancellationToken cancellationToken)
    {
        var sql = $"SELECT Id, Year, EssayType, EssayContent, ReaderId, StudentId, GoldScore FROM {_options.EssaysTable}";
        var connection = _connectionFactory();
        if (connection is not System.Data.Common.DbConnection dbConnection)
        {
            connection.Dispose();
            throw new InvalidOperationException("Connection factory must return a DbConnection instance.");
        }

        await using (dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken);
            var result = await dbConnection.QueryAsync<EssayRecord>(new CommandDefinition(sql, cancellationToken: cancellationToken));
            return result.AsList();
        }
    }
}
