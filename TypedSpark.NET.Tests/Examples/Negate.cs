using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class Negate
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    IntegerColumn col = 1;
                    DataFrame result = df.Select(-col, col.Negate());

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

                    IntegerColumn col = -1;
                    DataFrame result = df.Select(-col, col.Negate());

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();

        [Fact]
        public static void Case3() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example3

                    IntegerColumn col = Functions.Null<IntegerColumn>();
                    DataFrame result = df.Select(-col, col.Negate());

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
