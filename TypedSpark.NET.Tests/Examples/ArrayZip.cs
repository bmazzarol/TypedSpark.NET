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
    public static class ArrayZip
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                var array1 = ArrayColumn.New<IntegerColumn>(1, 2, 3);
                var array2 = ArrayColumn.New<IntegerColumn>(2, 3, 4);
                DataFrame result = df.Select(array1.Zip(array2));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                var array1 = ArrayColumn.New<IntegerColumn>(1, 2, 3, 4);
                var array2 = ArrayColumn.New<DoubleColumn>(2.0, 3.2, 4.5);
                var array3 = ArrayColumn.New<StringColumn>("a", "b");
                DataFrame result = df.Select(array1.Zip(array2, array3));

                #endregion

                return result;
            });
    }
}
