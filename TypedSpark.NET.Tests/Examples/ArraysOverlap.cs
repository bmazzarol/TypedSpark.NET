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
    public static class ArraysOverlap
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                var array1 = ArrayColumn.New<IntegerColumn>(1, 2, 3);
                var array2 = ArrayColumn.New<IntegerColumn>(3, 4, 5);
                DataFrame result = df.Select(array1.Overlaps(array2));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                var array1 = ArrayColumn.New<IntegerColumn>(1, 2, 3);
                var array2 = ArrayColumn.New<IntegerColumn>(4, 5, 6);
                DataFrame result = df.Select(array1.Overlaps(array2));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case3() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example3

                var array1 = ArrayColumn.New<IntegerColumn>(1, 2, 3);
                var array2 = ArrayColumn.New(4, 5, 6, Functions.Null<IntegerColumn>());
                DataFrame result = df.Select(array1.Overlaps(array2));

                #endregion

                return result;
            });
    }
}
