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
    public static class NumericColumnTests
    {
        [Fact(DisplayName = "Abs can be called on numeric columns")]
        public static async Task Case1() =>
            await DebugDataframe(s =>
            {
                IntegerColumn a = -1;
                IntegerColumn b = 1;
                DoubleColumn c = -123.00;
                return s.CreateEmptyFrame().Select(a, a.Abs(), b, b.Abs(), c, c.Abs());
            });

        [Fact(DisplayName = "Acos can be called on numeric columns")]
        public static async Task Case2() =>
            await DebugDataframe(s =>
            {
                IntegerColumn a = 1;
                IntegerColumn b = 2;
                DoubleColumn c = 1.2;
                return s.CreateEmptyFrame()
                    .Select(a, a.Acos(), b, b.Acos(), c, c.Acos(), c.Acos().IsNaN());
            });

        [Fact(DisplayName = "Acosh can be called on numeric columns")]
        public static async Task Case3() =>
            await DebugDataframe(s =>
            {
                IntegerColumn a = 1;
                IntegerColumn b = 2;
                DoubleColumn c = 1.2;
                return s.CreateEmptyFrame()
                    .Select(a, a.Acosh(), b, b.Acosh(), c, c.Acosh(), c.Acosh().IsNaN());
            });
    }
}
