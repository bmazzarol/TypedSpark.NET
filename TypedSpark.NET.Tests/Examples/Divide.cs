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
    public static class Divide
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                IntegerColumn a = 3;
                IntegerColumn b = 2;
                DataFrame result = df.Select(
                    a,
                    b,
                    a / b,
                    a.Divide(b),
                    (3 / b).As("left literal"),
                    (a / 2).As("right literal")
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

                LongColumn a = 2;
                LongColumn b = 2;
                DataFrame result = df.Select(
                    a,
                    b,
                    a / b,
                    a.Divide(b),
                    (2 / b).As("left literal"),
                    (a / 2).As("right literal")
                );

                #endregion

                return result;
            });
    }
}
