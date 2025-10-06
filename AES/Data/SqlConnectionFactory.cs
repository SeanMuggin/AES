using System;
using Azure.Core;
using Azure.Identity;
using Microsoft.Data.SqlClient;

namespace AES.Evaluator.Data;

internal sealed class SqlConnectionFactory
{
    private static readonly TokenRequestContext TokenContext = new(new[] { "https://database.windows.net/.default" });

    private readonly string _connectionString;
    private readonly TokenCredential _credential;

    public SqlConnectionFactory(string connectionString, TokenCredential? credential = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("A SQL connection string is required.", nameof(connectionString));
        }

        _connectionString = connectionString;
        _credential = credential ?? new DefaultAzureCredential();
    }

    public async Task<SqlConnection> CreateOpenConnectionAsync(CancellationToken cancellationToken)
    {
        //TokenCredential tokenCredential = _credential;
        //string tokenString = tokenCredential.GetToken(default, new()).Token;
        var connection = new SqlConnection(_connectionString);
        var token = await _credential.GetTokenAsync(TokenContext, cancellationToken).ConfigureAwait(false);
        connection.AccessToken = token.Token;
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }
}
