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
    public static class ArrayJoin
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                ArrayColumn<StringColumn> array = new StringColumn[] { "hello", "world" };
                DataFrame result = df.Select(array.Join(" "), Functions.Join(array, ";"));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                ArrayColumn<StringColumn> array = new[]
                {
                    "hello",
                    Functions.Null<StringColumn>(),
                    "world"
                };
                DataFrame result = df.Select(array.Join(" "));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case3() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example3

                ArrayColumn<StringColumn> array = new[]
                {
                    "hello",
                    Functions.Null<StringColumn>(),
                    "world"
                };
                DataFrame result = df.Select(array.Join(" ", ","));

                #endregion

                return result;
            });
    }
}
