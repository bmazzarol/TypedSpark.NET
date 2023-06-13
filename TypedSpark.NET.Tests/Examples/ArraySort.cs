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
    public static class ArraySort
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                ArrayColumn<IntegerColumn> array = new IntegerColumn[] { 5, 6, 1 };
                DataFrame result = df.Select(
                    array.Sort(
                        (left, right) =>
                            Functions
                                .When<IntegerColumn>(left < right, -1)
                                .When(left > right, 1)
                                .Otherwise(0)
                    )
                );

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example2

                var df = s.CreateDataFrameFromData(new { array = new[] { "bc", "ab", "dc" } });
                var array = ArrayColumn.New<StringColumn>("array");
                DataFrame result = df.Select(
                    array.Sort(
                        (left, right) =>
                            Functions
                                .When<IntegerColumn>(left.IsNull() & right.IsNull(), 0)
                                .When(left.IsNull(), -1)
                                .When(right.IsNull(), 1)
                                .When(left < right, 1)
                                .When(left > right, -1)
                                .Otherwise(0)
                    )
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

                ArrayColumn<StringColumn> array = new[]
                {
                    "b",
                    "d",
                    Functions.Null<StringColumn>(),
                    "c",
                    "a"
                };
                DataFrame result = df.Select(array.Sort());

                #endregion

                return result;
            });
    }
}
