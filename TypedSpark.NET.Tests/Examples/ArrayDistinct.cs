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
    public static class ArrayDistinct
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                var array = ArrayColumn.New<IntegerColumn>(
                    1,
                    2,
                    3,
                    Functions.Null<IntegerColumn>(),
                    3
                );
                DataFrame result = df.Select(array.Distinct());

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                var array = ArrayColumn.New<StringColumn>(
                    "a",
                    "b",
                    "a",
                    "c",
                    Functions.Null<StringColumn>(),
                    Functions.Null<StringColumn>(),
                    "c",
                    "b"
                );
                DataFrame result = df.Select(array.Distinct());

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
                ArrayColumn<DateColumn> array = ArrayColumn.New(Enumerable.Repeat(date, 3));
                DataFrame result = df.Select(array.Distinct());

                #endregion

                return result;
            });
    }
}
