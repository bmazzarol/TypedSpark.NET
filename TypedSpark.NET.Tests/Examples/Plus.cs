﻿using System.Threading.Tasks;
using BunsenBurner;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using VerifyXunit;
using Xunit;
using static TypedSpark.NET.Tests.Examples.ExampleExtensions;

namespace TypedSpark.NET.Tests.Examples
{
    [UsesVerify]
    public static class Plus
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                IntegerColumn a = 1;
                IntegerColumn b = 2;
                DataFrame result = df.Select(
                    a + b,
                    a.Plus(b),
                    (1 + b).As("left literal"),
                    (a + 2).As("right literal")
                );

                #endregion

                return result;
            });
    }
}
