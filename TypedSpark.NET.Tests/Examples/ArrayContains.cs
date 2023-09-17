using System;
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
    public static class ArrayContains
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                ArrayColumn<IntegerColumn> array = new IntegerColumn[] { 1, 2, 3 };
                DataFrame result = df.Select(
                    array.Contains(2),
                    array.Contains(5),
                    Functions.Contains(array, 3)
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

                var array = ArrayColumn.New<StringColumn>("a", "b", "c");
                DataFrame result = df.Select(array.Contains("a"), array.Contains("d"));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case3() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example3

                DateColumn date = new DateTime(2023, 3, 10);
                ArrayColumn<DateColumn> array = ArrayColumn.New(
                    Enumerable.Range(1, 4).Select(x => date.AddMonths(x))
                );
                DataFrame result = df.Select(
                    array.Contains(new DateTime(2023, 4, 10)),
                    array.Contains(new DateTime(2023, 12, 10))
                );

                #endregion

                return result;
            });
    }
}
