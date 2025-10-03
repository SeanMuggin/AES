using System.Linq;
using AES.Evaluator.Models;

namespace AES.Evaluator.Metrics;

public static class MetricsCalculator
{
    public static double Accuracy(IReadOnlyList<int> actual, IReadOnlyList<int> predicted)
    {
        if (actual.Count == 0 || actual.Count != predicted.Count)
        {
            return double.NaN;
        }

        var correct = actual.Zip(predicted, (a, p) => a == p ? 1 : 0).Sum();
        return (double)correct / actual.Count;
    }

    public static double QuadraticWeightedKappa(IReadOnlyList<int> actual, IReadOnlyList<int> predicted, int minScore = 1, int maxScore = 5)
    {
        if (actual.Count == 0 || actual.Count != predicted.Count)
        {
            return double.NaN;
        }

        var categories = maxScore - minScore + 1;
        if (categories <= 1)
        {
            return 1.0;
        }

        var matrix = new double[categories, categories];
        for (var i = 0; i < actual.Count; i++)
        {
            var row = actual[i] - minScore;
            var col = predicted[i] - minScore;
            if (row < 0 || row >= categories || col < 0 || col >= categories)
            {
                continue;
            }

            matrix[row, col] += 1;
        }

        var total = actual.Count;
        var rowSums = new double[categories];
        var colSums = new double[categories];
        for (var i = 0; i < categories; i++)
        {
            for (var j = 0; j < categories; j++)
            {
                rowSums[i] += matrix[i, j];
                colSums[j] += matrix[i, j];
            }
        }

        var observed = 0.0;
        var expected = 0.0;
        var denominator = Math.Pow(categories - 1, 2);
        for (var i = 0; i < categories; i++)
        {
            for (var j = 0; j < categories; j++)
            {
                var weight = Math.Pow(i - j, 2) / denominator;
                var observedProbability = matrix[i, j] / total;
                var expectedProbability = (rowSums[i] * colSums[j]) / Math.Pow(total, 2);
                observed += weight * observedProbability;
                expected += weight * expectedProbability;
            }
        }

        if (Math.Abs(expected) < double.Epsilon)
        {
            return 1.0;
        }

        return 1.0 - (observed / expected);
    }

    public static double MacroF1(IReadOnlyList<int> actual, IReadOnlyList<int> predicted, int minScore = 1, int maxScore = 5)
    {
        if (actual.Count == 0 || actual.Count != predicted.Count)
        {
            return double.NaN;
        }

        var categories = maxScore - minScore + 1;
        double totalF1 = 0;
        for (var score = minScore; score <= maxScore; score++)
        {
            var tp = 0;
            var fp = 0;
            var fn = 0;
            for (var i = 0; i < actual.Count; i++)
            {
                var a = actual[i];
                var p = predicted[i];
                if (p == score && a == score)
                {
                    tp++;
                }
                else if (p == score && a != score)
                {
                    fp++;
                }
                else if (p != score && a == score)
                {
                    fn++;
                }
            }

            var precision = tp + fp == 0 ? 0.0 : (double)tp / (tp + fp);
            var recall = tp + fn == 0 ? 0.0 : (double)tp / (tp + fn);
            var f1 = precision + recall == 0 ? 0.0 : 2 * precision * recall / (precision + recall);
            totalF1 += f1;
        }

        return totalF1 / categories;
    }

    public static double SpearmanCorrelation(IReadOnlyList<int> actual, IReadOnlyList<int> predicted)
    {
        if (actual.Count == 0 || actual.Count != predicted.Count)
        {
            return double.NaN;
        }

        var ranksActual = GetRanks(actual.Select(v => (double)v).ToArray());
        var ranksPredicted = GetRanks(predicted.Select(v => (double)v).ToArray());

        var meanActual = ranksActual.Average();
        var meanPredicted = ranksPredicted.Average();

        double numerator = 0;
        double sumSqActual = 0;
        double sumSqPredicted = 0;
        for (var i = 0; i < ranksActual.Length; i++)
        {
            var diffActual = ranksActual[i] - meanActual;
            var diffPredicted = ranksPredicted[i] - meanPredicted;
            numerator += diffActual * diffPredicted;
            sumSqActual += Math.Pow(diffActual, 2);
            sumSqPredicted += Math.Pow(diffPredicted, 2);
        }

        var denominator = Math.Sqrt(sumSqActual * sumSqPredicted);
        if (denominator == 0)
        {
            return double.NaN;
        }

        return numerator / denominator;
    }

    public static int[,] BuildConfusionMatrix(IReadOnlyList<int> actual, IReadOnlyList<int> predicted, int minScore = 1, int maxScore = 5)
    {
        var size = maxScore - minScore + 1;
        if (size <= 0)
        {
            return new int[0, 0];
        }
        var matrix = new int[size, size];
        for (var i = 0; i < actual.Count; i++)
        {
            var row = actual[i] - minScore;
            var col = predicted[i] - minScore;
            if (row < 0 || row >= size || col < 0 || col >= size)
            {
                continue;
            }

            matrix[row, col]++;
        }

        return matrix;
    }

    private static double[] GetRanks(double[] values)
    {
        var indexed = values.Select((value, index) => (value, index)).OrderBy(t => t.value).ToArray();
        var ranks = new double[values.Length];
        var i = 0;
        while (i < indexed.Length)
        {
            var j = i + 1;
            while (j < indexed.Length && Math.Abs(indexed[j].value - indexed[i].value) < double.Epsilon)
            {
                j++;
            }

            var rank = (i + j + 1) / 2.0;
            for (var k = i; k < j; k++)
            {
                ranks[indexed[k].index] = rank;
            }

            i = j;
        }

        return ranks;
    }
}
