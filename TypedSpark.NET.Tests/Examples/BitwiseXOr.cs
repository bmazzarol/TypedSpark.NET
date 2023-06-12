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
    public static class BitwiseXOr
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example1

                var df = s.CreateDataFrameFromData(new { x = 3 }, new { x = 5 });
                IntegerColumn x = IntegerColumn.New("x");
                DataFrame result = df.Select(x.BitwiseXOR());

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                IntegerColumn a = 3;
                IntegerColumn b = 5;
                DataFrame result = df.Select(
                    a ^ b,
                    a.BitwiseXOR(b),
                    (3 ^ b).As("left literal"),
                    (a ^ 5).As("right literal")
                );

                #endregion

                return result;
            });
    }
}
