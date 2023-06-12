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
    public static class ApproxCountDistinct
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example1

                var df = s.CreateDataFrameFromData(new[] { 1, 1, 2, 2, 3 }.Select(x => new { x }));
                IntegerColumn x = IntegerColumn.New("x");
                DataFrame result = df.Select(x.ApproxCountDistinct());

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example2

                var df = s.CreateDataFrameFromData(
                    new[] { "a", "a", "b", "b", "c" }.Select(x => new { x })
                );
                StringColumn x = StringColumn.New("x");
                DataFrame result = df.Select(x.ApproxCountDistinct(0.2));

                #endregion

                return result;
            });
    }
}
