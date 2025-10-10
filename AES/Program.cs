using AES.Evaluator.Configuration;
using AES.Evaluator.Data;
using AES.Evaluator.Services;
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

Console.WriteLine($"Running in {options.Mode} mode.");

if (string.IsNullOrWhiteSpace(options.AzureOpenAi.Endpoint) || string.IsNullOrWhiteSpace(options.AzureOpenAi.ApiKey))
{
    Console.Error.WriteLine("Azure OpenAI endpoint and API key are required. Set AesEvaluator:AzureOpenAi:Endpoint and ApiKey in configuration.");
    return 1;
}

if (string.IsNullOrWhiteSpace(options.SqlDatabase.ConnectionString))
{
    Console.Error.WriteLine("SQL database connection string is required. Set AesEvaluator:SqlDatabase:ConnectionString in configuration.");
    return 1;
}

IDataRepository repository;
try
{
    repository = new SqlDataWarehouseRepository(options);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Failed to create SQL data repository: {ex.Message}");
    return 1;
}

SqlPipelineResultWriter writerInstance;
try
{
    writerInstance = new SqlPipelineResultWriter(options.SqlDatabase, options.Mode);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Failed to create SQL pipeline result writer: {ex.Message}");
    return 1;
}

using var writer = writerInstance;

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
