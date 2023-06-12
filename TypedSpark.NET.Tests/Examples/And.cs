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
    public static class And
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                BooleanColumn a = true;
                BooleanColumn b = true;
                DataFrame result = df.Select(
                    a & b,
                    a.And(b),
                    (true & b).As("left literal"),
                    (a & true).As("right literal")
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

                BooleanColumn a = true;
                BooleanColumn b = false;
                DataFrame result = df.Select(
                    a & b,
                    a.And(b),
                    (true & b).As("left literal"),
                    (a & false).As("right literal")
                );

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case3() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example3

                BooleanColumn a = true;
                BooleanColumn b = Functions.Null<BooleanColumn>();
                DataFrame result = df.Select(a & b, a.And(b));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case4() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example4

                BooleanColumn a = false;
                BooleanColumn b = Functions.Null<BooleanColumn>();
                DataFrame result = df.Select(a & b, a.And(b));

                #endregion

                return result;
            });
    }
}
