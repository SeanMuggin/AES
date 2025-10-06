using System;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AES.Evaluator.Configuration;
using AES.Evaluator.Data;
using Azure.Core;
using Xunit;

namespace AES.Tests.Data;

public class FabricLakehousePipelineResultWriterTests
{
    [Fact]
    public async Task UploadToLakehouseAsync_UploadsCsvContentThroughFactory()
    {
        var options = new AesEvaluatorOptions.DatabaseOptions
        {
            RubricsTableEndpoint = "https://contoso/workspace-id/lakehouse-id/tables/rubrics",
            PredictionsTable = "predictions",
            UsageTable = "usage",
            MetricsByRubricTable = "metrics"
        };

        var credential = new TestTokenCredential();
        var factory = new TestLakehouseClientFactory();

        using var writer = new FabricLakehousePipelineResultWriter(
            options,
            httpClient: new HttpClient(new HttpClientHandler()),
            credential: credential,
            clientFactory: factory);

        var uploadMethod = typeof(FabricLakehousePipelineResultWriter).GetMethod(
            "UploadToLakehouseAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(uploadMethod);

        var relativePath = "Files/results/predictions.csv";
        const string csvContent = "Id,Value\n1,test";

        var invocation = uploadMethod!.Invoke(
            writer,
            new object[] { relativePath, csvContent, CancellationToken.None });

        var task = Assert.IsType<Task>(invocation);
        await task.ConfigureAwait(false);

        Assert.Equal(
            new Uri("https://onelake.dfs.fabric.microsoft.com/workspace-id/lakehouse-id.Lakehouse"),
            factory.LastUri);
        Assert.Same(credential, factory.LastCredential);

        Assert.Equal("Files/results", factory.DirectoryClient.RequestedPath);
        Assert.Equal(1, factory.DirectoryClient.CreateIfNotExistsCount);
        Assert.Equal(CancellationToken.None, factory.DirectoryClient.LastCreateIfNotExistsToken);

        var fileClient = factory.DirectoryClient.FileClient;
        Assert.Equal("predictions.csv", fileClient.FileName);
        Assert.True(fileClient.Overwrite);
        Assert.Equal(CancellationToken.None, fileClient.LastUploadToken);
        Assert.Equal(csvContent, fileClient.UploadedContent);
    }

    private sealed class TestTokenCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            return new AccessToken("token", DateTimeOffset.UtcNow.AddMinutes(5));
        }

        public override ValueTask<AccessToken> GetTokenAsync(
            TokenRequestContext requestContext,
            CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(GetToken(requestContext, cancellationToken));
        }
    }

    private sealed class TestLakehouseClientFactory : ILakehouseClientFactory
    {
        public Uri? LastUri { get; private set; }

        public TokenCredential? LastCredential { get; private set; }

        public TestDirectoryClient DirectoryClient { get; } = new();

        public ILakehouseFileSystemClient Create(Uri fileSystemUri, TokenCredential credential)
        {
            LastUri = fileSystemUri;
            LastCredential = credential;
            return new TestFileSystemClient(DirectoryClient);
        }
    }

    private sealed class TestFileSystemClient : ILakehouseFileSystemClient
    {
        private readonly TestDirectoryClient _directoryClient;

        public TestFileSystemClient(TestDirectoryClient directoryClient)
        {
            _directoryClient = directoryClient;
        }

        public ILakehouseDirectoryClient GetDirectoryClient(string directoryPath)
        {
            _directoryClient.RequestedPath = directoryPath;
            return _directoryClient;
        }
    }

    private sealed class TestDirectoryClient : ILakehouseDirectoryClient
    {
        public string? RequestedPath { get; set; }

        public int CreateIfNotExistsCount { get; private set; }

        public CancellationToken LastCreateIfNotExistsToken { get; private set; }

        public TestFileClient FileClient { get; private set; } = new();

        public Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
        {
            CreateIfNotExistsCount++;
            LastCreateIfNotExistsToken = cancellationToken;
            return Task.CompletedTask;
        }

        public ILakehouseFileClient GetFileClient(string fileName)
        {
            FileClient = new TestFileClient { FileName = fileName };
            return FileClient;
        }
    }

    private sealed class TestFileClient : ILakehouseFileClient
    {
        public string? FileName { get; set; }

        public bool Overwrite { get; private set; }

        public CancellationToken LastUploadToken { get; private set; }

        public string UploadedContent { get; private set; } = string.Empty;

        public async Task UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken)
        {
            Overwrite = overwrite;
            LastUploadToken = cancellationToken;
            using var reader = new StreamReader(content, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
            UploadedContent = await reader.ReadToEndAsync().ConfigureAwait(false);
        }
    }
}
