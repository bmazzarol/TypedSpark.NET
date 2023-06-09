﻿using Docfx.ResultSnippets;
using FluentAssertions;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class BitwiseNot
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    IntegerColumn a = 0;
                    DataFrame result = df.Select(~a, a.BitwiseNot());

                    #endregion

                    result.Should().NotBeNull();

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}