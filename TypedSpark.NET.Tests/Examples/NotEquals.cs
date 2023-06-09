using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class NotEquals
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    IntegerColumn a = 1;
                    IntegerColumn b = 2;
                    StringColumn c = "2";
                    DataFrame result = df.Select(a != b, a != c);

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

                    IntegerColumn a = 1;
                    IntegerColumn b = 1;
                    StringColumn c = "1";
                    DataFrame result = df.Select(a != b, a != c);

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
