using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class BitwiseXOr
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    #region Example1

                    var df = s.CreateDataFrameFromData(new { x = 3 }, new { x = 5 });
                    IntegerColumn x = IntegerColumn.New("x");
                    DataFrame result = df.Select(x.BitwiseXOR());

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();

        [Fact]
        public static void Case2() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example2

                    IntegerColumn a = 3;
                    IntegerColumn b = 5;
                    DataFrame result = df.Select(a ^ b, a.BitwiseXOR(b));

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
