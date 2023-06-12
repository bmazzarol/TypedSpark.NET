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
    public static class Equals
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                IntegerColumn a = 2;
                IntegerColumn b = 2;
                DataFrame result = df.Select(
                    a == b,
                    a.EqualTo(b),
                    (2 == b).As("left literal"),
                    (a == 2).As("right literal")
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
                StringColumn b = "1";
                DataFrame result = df.Select(a, b, a == b);

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
                DataFrame result = df.Select(a == b);

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case4() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example4

                BooleanColumn a = Functions.Null<BooleanColumn>();
                BooleanColumn b = Functions.Null<BooleanColumn>();
                DataFrame result = df.Select(a == b);

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case5() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example5

                IntegerColumn a = 2;
                IntegerColumn b = 2;
                DataFrame result = df.Select(a.EqNullSafe(b));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case6() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example6

                IntegerColumn a = 1;
                StringColumn b = "1";
                DataFrame result = df.Select(a, b, a.EqNullSafe(b.CastToInteger()));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case7() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example7

                BooleanColumn a = true;
                BooleanColumn b = Functions.Null<BooleanColumn>();
                DataFrame result = df.Select(a.EqNullSafe(b));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case8() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example8

                BooleanColumn a = Functions.Null<BooleanColumn>();
                BooleanColumn b = Functions.Null<BooleanColumn>();
                DataFrame result = df.Select(a.EqNullSafe(b));

                #endregion

                return result;
            });
    }
}
