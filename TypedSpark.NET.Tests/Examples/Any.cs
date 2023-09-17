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
    public static class Any
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example1

                DataFrame df = s.CreateDataFrameFromData(
                    new[] { true, false, false }.Select(x => new { x })
                );
                BooleanColumn x = BooleanColumn.New("x");
                DataFrame result = df.Select(x.Any(), Functions.Any(x));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example2

                DataFrame df = s.CreateDataFrameFromData(
                    new bool?[] { null, true, false }.Select(x => new { x })
                );
                BooleanColumn x = BooleanColumn.New("x");
                DataFrame result = df.Select(x.Any());

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case3() =>
            await DebugDataframeAndSaveExample(s =>
            {
                #region Example3

                DataFrame df = s.CreateDataFrameFromData(
                    new bool?[] { false, false, null }.Select(x => new { x })
                );
                BooleanColumn x = BooleanColumn.New("x");
                DataFrame result = df.Select(x.Any());

                #endregion

                return result;
            });
    }
}
