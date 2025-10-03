using System.Data;
using AES.Evaluator.Configuration;
using AES.Evaluator.Data;
using AES.Evaluator.Services;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .AddEnvironmentVariables()
    .AddCommandLine(args)
    .Build();

var options = configuration.GetSection(AesEvaluatorOptions.SectionName).Get<AesEvaluatorOptions>();
if (options is null)
{
    Console.Error.WriteLine("Configuration section 'AesEvaluator' is missing or invalid.");
    return 1;
}

if (string.IsNullOrWhiteSpace(options.Database.ConnectionString))
{
    Console.Error.WriteLine("Database connection string is required. Set AesEvaluator:Database:ConnectionString in configuration.");
    return 1;
}

if (string.IsNullOrWhiteSpace(options.AzureOpenAi.Endpoint) || string.IsNullOrWhiteSpace(options.AzureOpenAi.ApiKey))
{
    Console.Error.WriteLine("Azure OpenAI endpoint and API key are required. Set AesEvaluator:AzureOpenAi:Endpoint and ApiKey in configuration.");
    return 1;
}

IDataRepository repository = new SqlDataRepository(options.Database, CreateConnection);
IPipelineResultWriter writer = new SqlPipelineResultWriter(options.Database);

using var scorer = new AzureOpenAiScorer(options.AzureOpenAi, options.Execution.MaxRetries);
var runSeed = new Random().Next(1000, 9999);
var pipeline = new EssayScoringPipeline(repository, scorer, writer, options, runSeed);

try
{
    await pipeline.RunAsync(CancellationToken.None);
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Pipeline failed: {ex.Message}");
    Console.Error.WriteLine(ex);
    return 1;
}

SqlConnection CreateConnection()
{
    return new SqlConnection(options.Database.ConnectionString);
}
