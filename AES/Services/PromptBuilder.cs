using System.Text.Json;
using System.Text.Json.Nodes;
using AES.Evaluator.Models;

namespace AES.Evaluator.Services;

public static class PromptBuilder
{
    public const string SystemPrompt =
        "You are serving as a scholarship essay reviewer. Your role is to evaluate and score student essays based on the defined rubric.\n"
        + "For each essay, assign a score from 1 to 5 strictly per the rubric.\n"
        + "Return only a single JSON object with key 'results' containing an array of objects {id, score, rationale}.\n"
        + "Constraints: score is an integer 1..5; rationale is concise (1–2 sentences). No extra keys.\n"
        + "You may be given labeled examples. Use them to calibrate to the rubric, but do not copy text from examples.\n"
        + "If the essay lacks sufficient information to confidently assign a score, return the lowest score justified by available content.\n"
        + "If rationale is unrelated to academic or career success (e.g., sports), do not count it toward rubric criteria unless explicitly relevant.\n"
        + "Output must be only the JSON described.";

    public static string FormatExemplars(IReadOnlyCollection<(string Text, int Score)> exemplars)
    {
        var array = new JsonArray();
        foreach (var exemplar in exemplars)
        {
            var obj = new JsonObject
            {
                ["text"] = exemplar.Text,
                ["score"] = exemplar.Score
            };
            array.Add(obj);
        }

        return array.ToJsonString(new JsonSerializerOptions
        {
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = false
        });
    }

    public static string BuildUserMessage(string rubric, IReadOnlyCollection<BatchItem> essays, IReadOnlyCollection<(string Text, int Score)>? exemplars)
    {
        var essaysArray = new JsonArray();
        foreach (var essay in essays)
        {
            var obj = new JsonObject
            {
                ["id"] = essay.Id,
                ["text"] = essay.Text
            };
            essaysArray.Add(obj);
        }

        var essayJson = essaysArray.ToJsonString(new JsonSerializerOptions
        {
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = false
        });

        var exemplarBlock = string.Empty;
        if (exemplars is { Count: > 0 })
        {
            exemplarBlock = $"Examples (labeled):\n{FormatExemplars(exemplars)}\n\n";
        }

        return $"Rubric:\n{rubric}\n\n" +
               exemplarBlock +
               "Task:\n" +
               "1) Score each essay 1–5 according to the rubric.\n" +
               "2) Return only a JSON object:{\"results\":[{\"id\":\"...\",\"score\":<1-5>,\"rationale\":\"...\"}, ...]}.\n" +
               "Do not include any other text or keys.\n\n" +
               $"Essays:\n{essayJson}";
    }
}
