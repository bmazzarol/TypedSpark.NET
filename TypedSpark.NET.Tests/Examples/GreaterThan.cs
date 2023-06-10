﻿using System;
using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class GreaterThan
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    IntegerColumn a = 2;
                    IntegerColumn b = 1;
                    DataFrame result = df.Select(
                        a > b,
                        a.Gt(b),
                        (2 > b).As("left literal"),
                        (a > 1).As("right literal")
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

                    IntegerColumn a = 2;
                    DoubleColumn b = 1.1;
                    DataFrame result = df.Select(a.CastToDouble() > b, a.CastToDouble().Gt(b));

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

                    TimestampColumn a = new DateTime(2009, 07, 30, 04, 17, 52);
                    TimestampColumn b = new DateTime(2009, 07, 30, 04, 17, 52);
                    DataFrame result = df.Select(a > b, a.Gt(b));

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

                    TimestampColumn a = new DateTime(2009, 07, 30, 04, 17, 52);
                    TimestampColumn b = new DateTime(2009, 08, 01, 04, 17, 52);
                    DataFrame result = df.Select(a > b, a.Gt(b));

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();

        [Fact]
        public static void Case5() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example5

                    IntegerColumn a = 1;
                    IntegerColumn b = Functions.Null<IntegerColumn>();
                    DataFrame result = df.Select(a > b, a.Gt(b));

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}