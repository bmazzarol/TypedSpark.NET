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
    public static class ArrayRemove
    {
        [Fact]
        public static async Task Case1() =>
            await DebugDataframeAndSaveExample(s =>
            {
                var df = s.CreateEmptyFrame();

                #region Example1

                ArrayColumn<IntegerColumn> array = new[]
                {
                    1,
                    2,
                    3,
                    Functions.Null<IntegerColumn>(),
                    3
                };
                DataFrame result = df.Select(array.Remove(3), array.Remove(4));

                #endregion

                return result;
            });
    }
}
