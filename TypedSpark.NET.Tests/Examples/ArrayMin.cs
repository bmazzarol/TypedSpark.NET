using System;
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
    public static class ArrayMin
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                ArrayColumn<IntegerColumn> array = new[]
                {
                    1,
                    20,
                    Functions.Null<IntegerColumn>(),
                    3
                };
                DataFrame result = df.Select(array.Min());

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                ArrayColumn<DoubleColumn> array = new[]
                {
                    1.2,
                    22.345,
                    Functions.Null<DoubleColumn>(),
                    3.9
                };
                DataFrame result = df.Select(array.Min());

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
                    "a",
                    "b",
                    Functions.Null<StringColumn>(),
                    "d"
                };
                DataFrame result = df.Select(array.Min());

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case4() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example4

                ArrayColumn<DateColumn> array = new[]
                {
                    new DateTime(2022, 1, 2),
                    new DateTime(2022, 2, 1),
                    Functions.Null<DateColumn>(),
                    new DateTime(2022, 2, 12)
                };
                DataFrame result = df.Select(array.Min());

                #endregion

                return result;
            });
    }
}
