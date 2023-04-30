using System.Text.RegularExpressions;
using System.Threading.Tasks;
using BunsenBurner;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using VerifyXunit;
using Xunit;
using static TypedSpark.NET.Tests.SparkTestExtensions;
using F = TypedSpark.NET.Functions;

namespace TypedSpark.NET.Tests.Columns
{
    [UsesVerify]
    public static class StringColumnTests
    {
        [Fact(DisplayName = "Sentences can be called on a string column")]
        public static async Task Case1() =>
            await DebugDataframe(s =>
            {
                StringColumn sentence =
                    "This is a test. It should be split into sentences and words.";
                return s.CreateEmptyFrame()
                    .Select(
                        sentence.As("Sentence"),
                        sentence.Sentences(),
                        sentence.Sentences("en", "au")
                    );
            });

        [Fact(DisplayName = "2 string columns can be added together")]
        public static async Task Case2() =>
            await DebugDataframe(s =>
            {
                StringColumn a = "This is a";
                StringColumn b = " test";
                var c = StringColumn.New("c");
                var d = StringColumn.New("d");
                return s.CreateDataFrameFromData(
                        new { c = "some other string ", d = "another string" }
                    )
                    .Select(
                        a,
                        b,
                        c,
                        d,
                        a + b,
                        c + "which is cool",
                        "some starting string to combine to " + d
                    );
            });

        [Fact(DisplayName = "Length can be called on a string column")]
        public static async Task Case3() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(new { a = "abcdefg" }).Select(a, a.Size());
            });

        [Fact(DisplayName = "Like can be called on a string column")]
        public static async Task Case4() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(
                        new { a = "Bill" },
                        new { a = "Jill" },
                        new { a = "Tom" },
                        new { a = "Fred" },
                        new { a = "James" }
                    )
                    .Select(a, a.Like("%ill"));
            });

        [Fact(DisplayName = "RLike can be called on a string column")]
        public static async Task Case5() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(
                        new { a = "Bill" },
                        new { a = "Jill" },
                        new { a = "Tom" },
                        new { a = "Fred" },
                        new { a = "James" }
                    )
                    .Select(a, a.RLike(new Regex("^[T|F].*$")));
            });

        [Fact(DisplayName = "Substr can be called on a string column")]
        public static async Task Case6() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                var b = IntegerColumn.New("b");
                return s.CreateDataFrameFromData(
                        new { a = "Bill", b = 2 },
                        new { a = "Jill", b = 1 },
                        new { a = "Tom", b = 3 },
                        new { a = "Fred", b = 0 },
                        new { a = "James", b = 3 }
                    )
                    .Select(a, b, a.SubStr(0, b), a.SubStr(b, 1));
            });

        [Fact(DisplayName = "Contains can be called on a string column")]
        public static async Task Case7() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(
                        new { a = "Bill" },
                        new { a = "Jill" },
                        new { a = "Tom" },
                        new { a = "Fred" },
                        new { a = "James" }
                    )
                    .Select(a, a.Contains("ill"));
            });

        [Fact(DisplayName = "{Starts|Ends}with can be called on a string column")]
        public static async Task Case8() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(
                        new { a = "Bill" },
                        new { a = "Jill" },
                        new { a = "Tom" },
                        new { a = "Fred" },
                        new { a = "James" }
                    )
                    .Select(a, a.StartsWith("B"), a.EndsWith("ill"));
            });

        [Fact(DisplayName = "Cast can be called on a string column")]
        public static async Task Case9() =>
            await DebugDataframe(s =>
            {
                StringColumn booleanC = "true";
                StringColumn intC = "1";
                StringColumn floatC = "1.0";
                StringColumn dateC = "2023-04-30";
                StringColumn tsC = "2023-04-30T12:00:00";
                return s.CreateEmptyFrame()
                    .Select(
                        booleanC.CastToBoolean(),
                        intC.CastToByte(),
                        intC.CastToShort(),
                        intC.CastToInteger(),
                        intC.CastToLong(),
                        floatC.CastToFloat(),
                        floatC.CastToDouble(),
                        floatC.CastToDecimal(),
                        dateC.CastToDate(),
                        tsC.CastToTimestamp()
                    );
            });

        [Fact(DisplayName = "IsIn can be called on a string column")]
        public static async Task Case10() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(
                        new { a = "Bill" },
                        new { a = "Jill" },
                        new { a = "Tom" },
                        new { a = "Fred" },
                        new { a = "James" }
                    )
                    .Select(a, a.IsIn("Fred", "James", "Fred"));
            });

        [Fact(DisplayName = "Ascii can be called on a string column")]
        public static async Task Case11() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(
                        new { a = "A" },
                        new { a = "a" },
                        new { a = "B" },
                        new { a = "b" },
                        new { a = "Zad" }
                    )
                    .Select(a, a.Ascii());
            });

        [Fact(DisplayName = "ConcatWs can be called on string columns")]
        public static async Task Case12() =>
            await DebugDataframe(
                s =>
                    s.CreateEmptyFrame()
                        .Select(
                            F.ConcatWs(
                                ",",
                                (StringColumn)"a",
                                (StringColumn)"b",
                                (StringColumn)"c",
                                (StringColumn)"d"
                            )
                        )
            );

        [Fact(DisplayName = "Case can be called on string columns")]
        public static async Task Case13() =>
            await DebugDataframe(s =>
            {
                StringColumn test = "test";
                StringColumn upper = "TEST";
                return s.CreateEmptyFrame().Select(test.InitCap(), test.Upper(), upper.Lower());
            });

        [Fact(DisplayName = "Locate can be called on string columns")]
        public static async Task Case14() =>
            await DebugDataframe(s =>
            {
                StringColumn test = "Some test string to test on";
                return s.CreateEmptyFrame()
                    .Select(
                        test,
                        test.Locate("test"),
                        test.Locate("test", 10),
                        test.Locate("test", 22),
                        test.Locate("unknown")
                    );
            });

        [Fact(DisplayName = "Trim and pad can be called on string columns")]
        public static async Task Case15() =>
            await DebugDataframe(s =>
            {
                StringColumn test = " test ";
                return s.CreateEmptyFrame()
                    .Select(
                        test,
                        test.Ltrim(),
                        test.Rtrim(),
                        test.Trim(),
                        test.Lpad(8, "~"),
                        test.Rpad(8, "~"),
                        ("###" + test).Ltrim("#"),
                        (test + "###").Rtrim("#")
                    );
            });

        [Fact(DisplayName = "Coalesce can be called on string columns")]
        public static async Task Case16() =>
            await DebugDataframe(s =>
            {
                var a = StringColumn.New("a");
                return s.CreateDataFrameFromData(
                        new { a = "Bill" },
                        new { a = "Jill" },
                        new { a = (string?)null },
                        new { a = "Tom" },
                        new { a = "Fred" },
                        new { a = "James" },
                        new { a = (string?)null }
                    )
                    .Select(a, F.Coalesce(a, "Unknown"));
            });
    }
}
