using System;
using AES.Evaluator.Data;
using Xunit;

namespace AES.Tests.Data;

public class SqlIdentifierHelperTests
{

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
