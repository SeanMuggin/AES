using System;
using AES.Evaluator.Data;
using Xunit;

namespace AES.Tests.Data;

public class SqlIdentifierHelperTests
{
    [Theory]
    [InlineData("rubric", "[rubric]")]
    [InlineData("dbo.rubric", "[dbo].[rubric]")]
    [InlineData("[schema].table", "[[schema]].[table]")]
    [InlineData("schema.sub.table", "[schema].[sub].[table]")]
    public void FormatTableName_EscapesIdentifiers(string input, string expected)
    {
        var actual = SqlIdentifierHelper.FormatTableName(input);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(null!)]
    [InlineData("")]
    [InlineData(" ")]
    public void FormatTableName_InvalidInput_Throws(string input)
    {
        Assert.Throws<ArgumentException>(() => SqlIdentifierHelper.FormatTableName(input));
    }

    [Fact]
    public void ConvertToNullableInt_ParsesString()
    {
        var result = SqlIdentifierHelper.ConvertToNullableInt("42");
        Assert.Equal(42, result);
    }

    [Fact]
    public void ConvertToNullableString_Blank_ReturnsNull()
    {
        var result = SqlIdentifierHelper.ConvertToNullableString("   ");
        Assert.Null(result);
    }
}
