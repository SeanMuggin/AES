using System;
using System.Globalization;
using System.Linq;

namespace AES.Evaluator.Data;

internal static class SqlIdentifierHelper
{
    public static string FormatTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        var segments = tableName.Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
        {
            throw new ArgumentException("Table name must contain at least one identifier.", nameof(tableName));
        }

        return string.Join('.', segments.Select(EscapeIdentifier));
    }

    private static string EscapeIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("Identifier segment cannot be empty.", nameof(identifier));
        }

        return $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}";
    }

    public static string ConvertToString(object? value)
        => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

    public static string? ConvertToNullableString(object? value)
    {
        var text = Convert.ToString(value, CultureInfo.InvariantCulture);
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    public static int? ConvertToNullableInt(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        return value switch
        {
            int intValue => intValue,
            long longValue => (int)longValue,
            short shortValue => shortValue,
            byte byteValue => byteValue,
            sbyte sbyteValue => sbyteValue,
            decimal decimalValue => Convert.ToInt32(Math.Round(decimalValue, MidpointRounding.AwayFromZero)),
            double doubleValue => Convert.ToInt32(Math.Round(doubleValue, MidpointRounding.AwayFromZero)),
            float floatValue => Convert.ToInt32(Math.Round(floatValue, MidpointRounding.AwayFromZero)),
            string stringValue when int.TryParse(stringValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => null
        };
    }
}
