using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class ApproxCountDistinct
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    #region Example1

                    var df = s.CreateDataFrameFromData(
                        new { x = 1 },
                        new { x = 1 },
                        new { x = 2 },
                        new { x = 2 },
                        new { x = 3 }
                    );
                    IntegerColumn x = IntegerColumn.New("x");
                    DataFrame result = df.Select(x.ApproxCountDistinct());

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();

        [Fact]
        public static void Case2() =>
            UseSession(s =>
                {
                    #region Example2

                    var df = s.CreateDataFrameFromData(
                        new { x = "a" },
                        new { x = "a" },
                        new { x = "b" },
                        new { x = "b" },
                        new { x = "c" }
                    );
                    StringColumn x = StringColumn.New("x");
                    DataFrame result = df.Select(x.ApproxCountDistinct(0.2));

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
