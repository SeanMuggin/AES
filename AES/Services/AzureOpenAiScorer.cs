using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Linq;
using AES.Evaluator.Configuration;
using AES.Evaluator.Models;

namespace AES.Evaluator.Services;

public sealed class AzureOpenAiScorer : IDisposable
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = null,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private readonly HttpClient _httpClient;
    private readonly Uri _requestUri;
    private readonly int _maxRetries;
    private bool _disposed;

    public AzureOpenAiScorer(AesEvaluatorOptions.AzureOpenAiOptions options, int maxRetries)
    {
        if (string.IsNullOrWhiteSpace(options.Endpoint))
        {
            throw new ArgumentException("Azure OpenAI endpoint must be provided.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.ApiKey))
        {
            throw new ArgumentException("Azure OpenAI API key must be provided.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.Model))
        {
            throw new ArgumentException("Azure OpenAI deployment/model name must be provided.", nameof(options));
        }

        var endpoint = options.Endpoint.EndsWith("/") ? options.Endpoint : options.Endpoint + "/";
        var apiVersion = string.IsNullOrWhiteSpace(options.ApiVersion) ? "2025-01-01-preview" : options.ApiVersion;
        _requestUri = new Uri($"{endpoint}openai/deployments/{options.Model}/chat/completions?api-version={apiVersion}");
        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Add("api-key", options.ApiKey);
        _httpClient.Timeout = TimeSpan.FromMinutes(20);
        _maxRetries = maxRetries;
    }

    public async Task<BatchScoreResult> ScoreBatchAsync(
        string rubric,
        IReadOnlyList<BatchItem> batch,
        IReadOnlyCollection<(string Text, int Score)>? exemplars,
        CancellationToken cancellationToken)
    {
        const int initialMaxTokens = 10000;
        var maxTokens = initialMaxTokens;
        var delay = TimeSpan.FromSeconds(1);
        Exception? lastException = null;

        for (var attempt = 0; attempt < _maxRetries; attempt++)
        {
            try
            {
                return await CallModelAsync(rubric, batch, exemplars, maxTokens, cancellationToken);
            }
            catch (JsonException ex)
            {
                lastException = ex;
                if (attempt < _maxRetries - 1)
                {
                    maxTokens = Math.Min(8192, (int)(maxTokens * 1.6));
                    await Task.Delay(delay, cancellationToken);
                    delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 8000));
                    continue;
                }

                if (batch.Count > 1)
                {
                    var midpoint = batch.Count / 2;
                    var left = await ScoreBatchAsync(rubric, batch.Take(midpoint).ToArray(), exemplars, cancellationToken);
                    var right = await ScoreBatchAsync(rubric, batch.Skip(midpoint).ToArray(), exemplars, cancellationToken);
                    return MergeResults(left, right);
                }

                throw;
            }
            catch (Exception ex) when (attempt < _maxRetries - 1)
            {
                lastException = ex;
                await Task.Delay(delay, cancellationToken);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 8000));
            }
        }

        throw lastException ?? new InvalidOperationException("Unknown error calling Azure OpenAI.");
    }

    private async Task<BatchScoreResult> CallModelAsync(
        string rubric,
        IReadOnlyList<BatchItem> batch,
        IReadOnlyCollection<(string Text, int Score)>? exemplars,
        int maxOutputTokens,
        CancellationToken cancellationToken)
    {
        var userMessage = PromptBuilder.BuildUserMessage(rubric, batch, exemplars);
        var payload = new
        {
            messages = new object[]
            {
                new { role = "system", content = PromptBuilder.SystemPrompt },
                new { role = "user", content = userMessage }
            },
            response_format = new { type = "json_object" },
            //temperature = 0,
            max_completion_tokens = maxOutputTokens
   //       max_tokens = maxOutputTokens //use for other models

        };

        using var request = new HttpRequestMessage(HttpMethod.Post, _requestUri)
        {
            Content = new StringContent(JsonSerializer.Serialize(payload, SerializerOptions), Encoding.UTF8, "application/json")
        };

        var start = DateTimeOffset.UtcNow;
        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var latencyMs = (int)(DateTimeOffset.UtcNow - start).TotalMilliseconds;
        var responseText = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"Azure OpenAI request failed with status {(int)response.StatusCode}: {responseText}");
        }

        using var document = JsonDocument.Parse(responseText);
        var root = document.RootElement;
        if (!root.TryGetProperty("choices", out var choices) || choices.ValueKind != JsonValueKind.Array || choices.GetArrayLength() == 0)
        {
            throw new JsonException("Azure OpenAI response did not contain choices.");
        }

        var message = choices[0].GetProperty("message");
        var content = ExtractContent(message);
        using var parsed = JsonDocument.Parse(content);
        if (!parsed.RootElement.TryGetProperty("results", out var resultsElement) || resultsElement.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("Azure OpenAI response missing 'results' array.");
        }

        var mapping = new Dictionary<string, int?>();
        var rationaleMap = new Dictionary<string, string?>();
        var raw = new List<JsonElement>();
        foreach (var element in resultsElement.EnumerateArray())
        {
            raw.Add(element.Clone());
            var id = element.TryGetProperty("id", out var idElement) ? idElement.GetString() ?? string.Empty : string.Empty;
            var scoreValue = element.TryGetProperty("score", out var scoreElement) ? ClampScore(scoreElement) : null;
            var rationale = element.TryGetProperty("rationale", out var rationaleElement) ? rationaleElement.GetString() : null;
            mapping[id.Trim()] = scoreValue;
            rationaleMap[id.Trim()] = rationale;
        }

        int? promptTokens = null;
        int? completionTokens = null;
        if (root.TryGetProperty("usage", out var usage))
        {
            if (usage.TryGetProperty("prompt_tokens", out var promptElement) && promptElement.TryGetInt32(out var prompt))
            {
                promptTokens = prompt;
            }

            if (usage.TryGetProperty("completion_tokens", out var completionElement) && completionElement.TryGetInt32(out var completion))
            {
                completionTokens = completion;
            }
        }

        return new BatchScoreResult(mapping, rationaleMap, latencyMs, promptTokens, completionTokens, raw);
    }

    private static string ExtractContent(JsonElement messageElement)
    {
        if (messageElement.TryGetProperty("content", out var contentElement))
        {
            return contentElement.ValueKind switch
            {
                JsonValueKind.Array => string.Concat(contentElement.EnumerateArray().Select(ExtractContentPart)),
                JsonValueKind.String => contentElement.GetString() ?? string.Empty,
                _ => contentElement.ToString()
            };
        }

        return string.Empty;
    }

    private static string ExtractContentPart(JsonElement part)
    {
        if (part.ValueKind == JsonValueKind.String)
        {
            return part.GetString() ?? string.Empty;
        }

        if (part.ValueKind == JsonValueKind.Object && part.TryGetProperty("text", out var textElement))
        {
            return textElement.GetString() ?? string.Empty;
        }

        return part.ToString();
    }

    private static int? ClampScore(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Number && element.TryGetInt32(out var numeric))
        {
            return Math.Clamp(numeric, 1, 5);
        }

        if (element.ValueKind == JsonValueKind.String && int.TryParse(element.GetString(), out var parsed))
        {
            return Math.Clamp(parsed, 1, 5);
        }

        return null;
    }

    private static BatchScoreResult MergeResults(BatchScoreResult left, BatchScoreResult right)
    {
        var mapping = left.Mapping.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        foreach (var (key, value) in right.Mapping)
        {
            mapping[key] = value;
        }

        var rationale = left.RationaleMap.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        foreach (var (key, value) in right.RationaleMap)
        {
            rationale[key] = value;
        }

        var raw = left.Raw.Concat(right.Raw).ToList();
        var latencyValues = new[] { left.LatencyMs, right.LatencyMs }
            .Where(v => v.HasValue)
            .Select(v => v!.Value)
            .ToList();
        int? latency = latencyValues.Count == 0 ? null : (int)Math.Round(latencyValues.Average());
        var inputTokens = SumNullable(left.InputTokens, right.InputTokens);
        var outputTokens = SumNullable(left.OutputTokens, right.OutputTokens);

        return new BatchScoreResult(mapping, rationale, latency, inputTokens, outputTokens, raw);
    }

    private static int? SumNullable(int? left, int? right)
    {
        if (!left.HasValue && !right.HasValue)
        {
            return null;
        }

        return (left ?? 0) + (right ?? 0);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _httpClient.Dispose();
        _disposed = true;
    }
}
