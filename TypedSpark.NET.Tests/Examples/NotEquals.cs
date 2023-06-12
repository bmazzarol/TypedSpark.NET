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
    public static class NotEquals
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                IntegerColumn a = 1;
                IntegerColumn b = 2;
                StringColumn c = "2";
                DataFrame result = df.Select(
                    a != b,
                    a != c,
                    a.NotEqual(b),
                    (1 != b).As("left literal"),
                    (a != 2).As("right literal")
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

                IntegerColumn a = 1;
                IntegerColumn b = 1;
                StringColumn c = "1";
                DataFrame result = df.Select(a != b, a != c);

                #endregion

                return result;
            });
    }
}
