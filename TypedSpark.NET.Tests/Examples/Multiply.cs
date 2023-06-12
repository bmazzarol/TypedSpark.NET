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
    public static class Multiply
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                IntegerColumn a = 2;
                IntegerColumn b = 3;
                DataFrame result = df.Select(
                    a * b,
                    a.Multiply(b),
                    (2 * b).As("left literal"),
                    (a * 3).As("right literal")
                );

                #endregion

                return result;
            });
    }
}
