using System;
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
    public static class AddMonths
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                DateColumn a = new DateTime(2016, 08, 31);
                IntegerColumn b = -1;
                DataFrame result = df.Select(a, a.AddMonths(1), b, Functions.AddMonths(a, b));

                #endregion

                return result;
            });

        [Fact]
        public static async Task Case2() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example2

                TimestampColumn a = new DateTime(2016, 08, 31, 12, 0, 0, DateTimeKind.Utc);
                IntegerColumn b = -1;
                DataFrame result = df.Select(a, a.AddMonths(1), b, Functions.AddMonths(a, b));

                #endregion

                return result;
            });
    }
}
