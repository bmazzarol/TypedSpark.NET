using System.Linq;
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
    public static class ApproxPercentile
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example1

                var df = s.CreateDataFrameFromData(new[] { 0, 1, 2, 10 }.Select(x => new { x }));
                IntegerColumn x = IntegerColumn.New("x");
                ArrayColumn<DoubleColumn> percentages = ArrayColumn.New<DoubleColumn>(
                    0.5,
                    0.4,
                    0.1
                );
                DataFrame result = df.Select(x.ApproxPercentile(percentages, 100));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example2

                var df = s.CreateDataFrameFromData(new[] { 0, 6, 7, 9, 10 }.Select(x => new { x }));
                IntegerColumn x = IntegerColumn.New("x");
                DoubleColumn percentage = 0.5;
                DataFrame result = df.Select(x.ApproxPercentile(percentage, 100));

                #endregion

                return result;
            });
    }
}
