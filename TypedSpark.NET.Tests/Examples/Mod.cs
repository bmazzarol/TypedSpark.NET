using System.Threading.Tasks;
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
    public static class Mod
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                DoubleColumn a = 2;
                DoubleColumn b = 1.8;
                DataFrame result = df.Select(
                    a % b,
                    a.Mod(b),
                    (2 % b).As("left literal"),
                    (a % 1.8).As("right literal")
                );

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                IntegerColumn a = 7;
                IntegerColumn b = 5;
                DataFrame result = df.Select(
                    a % b,
                    a.Mod(b),
                    (7 % b).As("left literal"),
                    (a % 5).As("right literal")
                );

                #endregion

                return result;
            });
    }
}
