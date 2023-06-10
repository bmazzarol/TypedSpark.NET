using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class And
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    BooleanColumn a = true;
                    BooleanColumn b = true;
                    DataFrame result = df.Select(
                        a & b,
                        a.And(b),
                        (true & b).As("left literal"),
                        (a & true).As("right literal")
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

                    BooleanColumn a = true;
                    BooleanColumn b = false;
                    DataFrame result = df.Select(
                        a & b,
                        a.And(b),
                        (true & b).As("left literal"),
                        (a & false).As("right literal")
                    );

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

                    BooleanColumn a = true;
                    BooleanColumn b = Functions.Null<BooleanColumn>();
                    DataFrame result = df.Select(a & b, a.And(b));

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();

        [Fact]
        public static void Case4() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example4

                    BooleanColumn a = false;
                    BooleanColumn b = Functions.Null<BooleanColumn>();
                    DataFrame result = df.Select(a & b, a.And(b));

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
