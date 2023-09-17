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
    public static class ArrayConcat
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                var array1 = ArrayColumn.New<IntegerColumn>(1, 2, 3);
                var array2 = ArrayColumn.New<IntegerColumn>(1, 3, 5);
                DataFrame result = df.Select(
                    array1 + array2,
                    array1.Concat(array2),
                    Functions.Concat(array1, array2)
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

                var array1 = ArrayColumn.New("a", Functions.Null<StringColumn>(), "b", "c");
                var array2 = ArrayColumn.New("a", "c", Functions.Null<StringColumn>(), "d");
                DataFrame result = df.Select(array1 + array2, array1.Concat(array2));

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
                ArrayColumn<DateColumn> array1 = ArrayColumn.New(
                    Enumerable.Range(1, 3).Select(x => date.AddMonths(x))
                );
                ArrayColumn<DateColumn> array2 = ArrayColumn.New(
                    Enumerable.Range(2, 5).Select(x => date.AddMonths(x))
                );
                DataFrame result = df.Select(array1 + array2, array1.Concat(array2));

                #endregion

                return result;
            });
    }
}
