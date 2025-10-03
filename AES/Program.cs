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

if (string.IsNullOrWhiteSpace(options.Database.RubricsTableEndpoint) ||
    string.IsNullOrWhiteSpace(options.Database.EssaysTableEndpoint))
{
    Console.Error.WriteLine("Fabric lakehouse table endpoints are required. Set AesEvaluator:Database:RubricsTableEndpoint and EssaysTableEndpoint in configuration.");
    return 1;
}

if (string.IsNullOrWhiteSpace(options.AzureOpenAi.Endpoint) || string.IsNullOrWhiteSpace(options.AzureOpenAi.ApiKey))
{
    Console.Error.WriteLine("Azure OpenAI endpoint and API key are required. Set AesEvaluator:AzureOpenAi:Endpoint and ApiKey in configuration.");
    return 1;
}

IDataRepository repository;
try
{
    repository = new FabricLakehouseDataRepository(options.Database);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Failed to create Fabric lakehouse data repository: {ex.Message}");
    return 1;
}

IPipelineResultWriter writer = string.IsNullOrWhiteSpace(options.Database.ConnectionString)
    ? new NullPipelineResultWriter()
    : new SqlPipelineResultWriter(options.Database);

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
