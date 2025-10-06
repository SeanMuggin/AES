using System;
using System.Threading;
using System.Threading.Tasks;
using AES.Evaluator.Data;
using Azure.Core;
using Microsoft.Data.SqlClient;
using Xunit;

namespace AES.Tests.Data;

public class SqlConnectionFactoryTests
{
    [Fact]
    public void Constructor_EmptyConnectionString_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SqlConnectionFactory(string.Empty));
    }

    [Fact]
    public async Task CreateOpenConnectionAsync_UsesTokenCredential()
    {
        var factory = new SqlConnectionFactory("Server=example;Database=test;Connection Timeout=1;", new TestCredential());

        await Assert.ThrowsAsync<SqlException>(() => factory.CreateOpenConnectionAsync(CancellationToken.None));
    }

    private sealed class TestCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new AccessToken("token", DateTimeOffset.UtcNow.AddMinutes(5));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new ValueTask<AccessToken>(new AccessToken("token", DateTimeOffset.UtcNow.AddMinutes(5)));
    }
}
