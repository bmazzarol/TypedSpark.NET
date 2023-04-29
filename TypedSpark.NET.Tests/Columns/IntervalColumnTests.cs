using System;
using System.Threading.Tasks;
using BunsenBurner;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using VerifyXunit;
using Xunit;
using static TypedSpark.NET.Tests.SparkTestExtensions;

namespace TypedSpark.NET.Tests.Columns
{
    [UsesVerify]
    public static class IntervalColumnTests
    {
        [Fact(DisplayName = "Day time interval columns can be created")]
        public static async Task Case1() =>
            await DebugDataframe(
                s => s.CreateEmptyFrame().Select(IntervalColumn.New(new TimeSpan(4, 3, 2, 14)))
            );

        [Fact(DisplayName = "Day time interval columns can be created subtracting 2 dates")]
        public static async Task Case2() =>
            await DebugDataframe(s =>
            {
                var d1 = new DateTime(2023, 04, 14);
                DateColumn dt1 = d1;
                var d2 = new DateTime(2023, 04, 29);
                DateColumn dt2 = d2;
                return s.CreateEmptyFrame()
                    .Select((dt2 - dt1).As("a"), (d2 - dt1).As("b"), (dt2 - d1).As("c"));
            });

        [Fact(DisplayName = "Day time interval columns can be created subtracting 2 timestamps")]
        public static async Task Case3() =>
            await DebugDataframe(s =>
            {
                var t1 = new DateTimeOffset(2023, 04, 14, 12, 2, 3, 4, TimeSpan.Zero);
                TimestampColumn dt1 = t1;
                var t2 = new DateTimeOffset(2023, 04, 29, 10, 3, 6, 3, TimeSpan.Zero);
                TimestampColumn dt2 = t2;
                return s.CreateEmptyFrame()
                    .Select((dt2 - dt1).As("a"), (t2 - dt1).As("b"), (dt2 - t1).As("c"));
            });
    }
}
