using System;
using System.Threading.Tasks;
using BunsenBurner;
using BunsenBurner.Verify.Xunit;
using Microsoft.Spark.Sql.Types;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using VerifyXunit;
using Xunit;
using static TypedSpark.NET.Tests.SparkTestExtensions;

namespace TypedSpark.NET.Tests.Columns
{
    [UsesVerify]
    public static class DateColumnTests
    {
        [Fact(DisplayName = "Year can be extracted from date columns")]
        public static async Task Case1() =>
            await ArrangeUsingSpark(s =>
                {
                    var d1 = new DateTime(2023, 04, 14);
                    DateColumn dt1 = d1;
                    return s.CreateEmptyFrame().Select(dt1, dt1.Year());
                })
                .Act(df => df.Debug())
                .AssertResultIsUnchanged();
    }
}
