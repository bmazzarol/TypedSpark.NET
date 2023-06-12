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
    public static class GreaterThan
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                IntegerColumn a = 2;
                IntegerColumn b = 1;
                DataFrame result = df.Select(
                    a > b,
                    a.Gt(b),
                    (2 > b).As("left literal"),
                    (a > 1).As("right literal")
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

                IntegerColumn a = 2;
                DoubleColumn b = 1.1;
                DataFrame result = df.Select(a.CastToDouble() > b, a.CastToDouble().Gt(b));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case3() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example3

                TimestampColumn a = new DateTime(2009, 07, 30, 04, 17, 52);
                TimestampColumn b = new DateTime(2009, 07, 30, 04, 17, 52);
                DataFrame result = df.Select(a > b, a.Gt(b));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case4() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example4

                TimestampColumn a = new DateTime(2009, 07, 30, 04, 17, 52);
                TimestampColumn b = new DateTime(2009, 08, 01, 04, 17, 52);
                DataFrame result = df.Select(a > b, a.Gt(b));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case5() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example5

                IntegerColumn a = 1;
                IntegerColumn b = Functions.Null<IntegerColumn>();
                DataFrame result = df.Select(a > b, a.Gt(b));

                #endregion

                return result;
            });
    }
}
