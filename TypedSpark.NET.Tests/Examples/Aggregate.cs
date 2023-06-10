using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class Aggregate
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    #region Example1

                    DataFrame df = s.CreateDataFrameFromData(new { array = new[] { 1, 2, 3 } });
                    ArrayColumn<IntegerColumn> array = ArrayColumn.New<IntegerColumn>("array");
                    IntegerColumn state = 0;
                    DataFrame result = df.Select(
                        array,
                        state.As("state"),
                        array.Aggregate(state, (acc, x) => acc + x).As("aggregate")
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
                    #region Example2

                    DataFrame df = s.CreateDataFrameFromData(new { array = new[] { 1, 2, 3 } });
                    ArrayColumn<IntegerColumn> array = ArrayColumn.New<IntegerColumn>("array");
                    IntegerColumn state = 0;
                    DataFrame result = df.Select(
                        array,
                        state.As("state"),
                        array.Aggregate(state, (acc, x) => acc + x, acc => acc * 10).As("aggregate")
                    );

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
