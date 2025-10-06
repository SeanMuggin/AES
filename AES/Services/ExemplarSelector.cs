using AES.Evaluator.Models;

namespace AES.Evaluator.Services;

public sealed class ExemplarSelector
{
    private readonly Random _random;

    public ExemplarSelector(int seed)
    {
        _random = new Random(seed);
    }

    public IReadOnlyCollection<(string Text, int Score)> ChooseExemplars(
        IReadOnlyCollection<EssayWithRubric> essays,
        IReadOnlySet<string> excludeIds,
        int k)
    {
        var pool = essays
            .Where(e => !excludeIds.Contains(e.Id) && e.GoldScore.HasValue)
            .ToList();

        if (pool.Count == 0 || k <= 0)
        {
            return Array.Empty<(string, int)>();
        }

        var result = new List<(string Text, int Score)>(k);

        for (var score = 1; score <= 5 && result.Count < k; score++)
        {
            var scoreGroup = pool.Where(e => e.GoldScore == score).ToList();
            if (scoreGroup.Count == 0)
            {
                continue;
            }

            var take = Math.Min(2, k - result.Count);
            AppendRandom(scoreGroup, take, result);
        }

        if (result.Count < k)
        {
            var remaining = pool
                .Where(e => !excludeIds.Contains(e.Id) && !result.Any(r => r.Text == e.EssayContent))
                .ToList();
            var needed = Math.Min(k - result.Count, remaining.Count);
            AppendRandom(remaining, needed, result);
        }

        return result.Take(k).ToArray();
    }

    private void AppendRandom(List<EssayWithRubric> candidates, int count, ICollection<(string Text, int Score)> target)
    {
        if (count <= 0 || candidates.Count == 0)
        {
            return;
        }

        Shuffle(candidates);
        foreach (var candidate in candidates.Take(count))
        {
            target.Add((candidate.EssayContent, candidate.GoldScore!.Value));
        }
    }

    private void Shuffle<T>(IList<T> list)
    {
        for (var i = list.Count - 1; i > 0; i--)
        {
            var j = _random.Next(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
    }
}
