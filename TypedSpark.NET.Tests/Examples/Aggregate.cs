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
    public static class Aggregate
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example1

                DataFrame df = s.CreateDataFrameFromData(new { array = new[] { 1, 2, 3 } });
                ArrayColumn<IntegerColumn> array = ArrayColumn.New<IntegerColumn>("array");
                IntegerColumn state = 0;
                DataFrame result = df.Select(
                    array,
                    state.As("state"),
                    array.Aggregate(state, (acc, x) => acc + x).As("aggregate")
                );

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example2

                DataFrame df = s.CreateDataFrameFromData(new { array = new[] { 1, 2, 3 } });
                ArrayColumn<IntegerColumn> array = ArrayColumn.New<IntegerColumn>("array");
                IntegerColumn state = 0;
                DataFrame result = df.Select(
                    array,
                    state.As("state"),
                    Functions
                        .Aggregate(
                            array,
                            state,
                            (acc, x) => acc + x,
                            acc => (acc * 10).CastToString()
                        )
                        .As("aggregate")
                );

                #endregion

                return result;
            });
    }
}
