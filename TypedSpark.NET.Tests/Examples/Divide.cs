using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class Divide
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    IntegerColumn a = 3;
                    IntegerColumn b = 2;
                    DataFrame result = df.Select(
                        a,
                        b,
                        a / b,
                        a.Divide(b),
                        (3 / b).As("left literal"),
                        (a / 2).As("right literal")
                    );

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

                    LongColumn a = 2;
                    LongColumn b = 2;
                    DataFrame result = df.Select(
                        a,
                        b,
                        a / b,
                        a.Divide(b),
                        (2 / b).As("left literal"),
                        (a / 2).As("right literal")
                    );

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
