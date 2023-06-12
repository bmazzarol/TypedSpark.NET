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
    public static class BitwiseOr
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                IntegerColumn a = 3;
                IntegerColumn b = 5;
                DataFrame result = df.Select(
                    (a | b).As("a xor b"),
                    a.BitwiseOR(b).As("a xor b"),
                    (3 | b).As("left literal"),
                    (a | 5).As("right literal")
                );

                #endregion

                return result;
            });
    }
}
