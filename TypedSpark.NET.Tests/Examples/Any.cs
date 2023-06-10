using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class Any
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    #region Example1

                    var df = s.CreateDataFrameFromData(
                        new { x = true },
                        new { x = false },
                        new { x = false }
                    );
                    BooleanColumn x = BooleanColumn.New("x");
                    DataFrame result = df.Select(x.Any());

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
                        new { x = null as bool? },
                        new { x = true as bool? },
                        new { x = false as bool? }
                    );
                    BooleanColumn x = BooleanColumn.New("x");
                    DataFrame result = df.Select(x.Any());

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();

        [Fact]
        public static void Case3() =>
            UseSession(s =>
                {
                    #region Example3

                    var df = s.CreateDataFrameFromData(
                        new { x = false as bool? },
                        new { x = false as bool? },
                        new { x = null as bool? }
                    );
                    BooleanColumn x = BooleanColumn.New("x");
                    DataFrame result = df.Select(x.Any());

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
