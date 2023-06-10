using System;
using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class AddMonths
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    DateColumn a = new DateTime(2016, 08, 31);
                    IntegerColumn b = -1;
                    DataFrame result = df.Select(a, a.AddMonths(1), b, a.AddMonths(b));

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

                    TimestampColumn a = new DateTime(2016, 08, 31, 12, 0, 0, DateTimeKind.Utc);
                    IntegerColumn b = -1;
                    DataFrame result = df.Select(a, a.AddMonths(1), b, a.AddMonths(b));

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
