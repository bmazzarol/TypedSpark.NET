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
    public static class ArrayColumnTests
    {
        [Fact(DisplayName = "Explode can be called on an array column")]
        public static async Task Case1() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col.Explode());
            });

        [Fact(DisplayName = "Index can be used to get a given element on an array column")]
        public static async Task Case2() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col[1], col[4]);
            });

        [Fact(
            DisplayName = "Array function can be used to create an array column from existing columns"
        )]
        public static async Task Case3() =>
            await DebugDataframe(s =>
            {
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        A = 1,
                        B = 2,
                        C = 3
                    },
                    new
                    {
                        A = 4,
                        B = 5,
                        C = 6
                    }
                );
                return df.Select(
                    ArrayColumn.New(
                        "D",
                        IntegerColumn.New("A"),
                        IntegerColumn.New("B"),
                        IntegerColumn.New("C")
                    )
                );
            });

        [Fact(
            DisplayName = "Array contains function can be used to test for a value in an array column"
        )]
        public static async Task Case4() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { a = 2, test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(
                    IntegerColumn.New("a"),
                    col,
                    col.Contains(3),
                    col.Contains(0),
                    col.Contains(IntegerColumn.New("a"))
                );
            });

        [Fact(
            DisplayName = "Array overlaps function can be used to test 2 array columns overlap on any values"
        )]
        public static async Task Case5() =>
            await DebugDataframe(s =>
            {
                var a = ArrayColumn.New<IntegerColumn>("a");
                var b = ArrayColumn.New<IntegerColumn>("b");
                var df = s.CreateDataFrameFromData(
                    new { a = new[] { 1, 2, 3, 4, 5 }, b = new[] { 1, 3, 5 } },
                    new { a = new[] { 1, 2, 3 }, b = new[] { 4, 5, 6 } }
                );
                return df.Select(a, b, a.Overlaps(b));
            });

        [Fact(DisplayName = "Array slice function can be used to slice and array column")]
        public static async Task Case6() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var a = IntegerColumn.New("a");
                var df = s.CreateDataFrameFromData(new { a = 2, test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(a, col, col.Slice(2, 2), col.Slice(a, a));
            });

        [Fact(DisplayName = "Array position function can be used to return index of given element")]
        public static async Task Case7() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col, col.Position(3), col.Position(6));
            });

        [Fact(DisplayName = "Array join can be used on array of string column")]
        public static async Task Case8() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<StringColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new { test = new[] { "a", "b", "c", "d", "e", null } }
                );
                return df.Select(col, col.Join(":", "f"));
            });

        [Fact(DisplayName = "Array sort can be used on array of order-able type column")]
        public static async Task Case9() =>
            await DebugDataframe(s =>
            {
                var saCol = ArrayColumn.New<StringColumn>("StringArray");
                var intCol = ArrayColumn.New<IntegerColumn>("IntegerArray");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        StringArray = new[] { "e", "d", "c", "b", "a" },
                        IntegerArray = new[] { 5, 4, 3, 2, 1 }
                    }
                );
                return df.Select(saCol, saCol.Sort(), intCol, intCol.Sort());
            });

        [Fact(DisplayName = "Array remove can be used on array column")]
        public static async Task Case10() =>
            await DebugDataframe(s =>
            {
                var a = IntegerColumn.New("a");
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { a = 3, test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(a, col, col.Remove(a), col.Remove(5));
            });

        [Fact(
            DisplayName = "Array distinct function can be used to remove duplicates from an array column"
        )]
        public static async Task Case11() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new { test = new[] { 1, 1, 1, 2, 2, 3, 4, 4, 5, 5, 5, 5 } }
                );
                return df.Select(col, col.Distinct());
            });

        [Fact(DisplayName = "Array intersect function can be used to intersect 2 array columns")]
        public static async Task Case12() =>
            await DebugDataframe(s =>
            {
                var a = ArrayColumn.New<IntegerColumn>("a");
                var b = ArrayColumn.New<IntegerColumn>("b");
                var df = s.CreateDataFrameFromData(
                    new { a = new[] { 1, 2, 3, 4 }, b = new[] { 3, 4, 5, 6 } }
                );
                return df.Select(a, b, a | b);
            });

        [Fact(DisplayName = "Array union function can be used to union 2 array columns")]
        public static async Task Case13() =>
            await DebugDataframe(s =>
            {
                var a = ArrayColumn.New<IntegerColumn>("a");
                var b = ArrayColumn.New<IntegerColumn>("b");
                var df = s.CreateDataFrameFromData(
                    new { a = new[] { 1, 2, 3, 4 }, b = new[] { 3, 4, 5, 6 } }
                );
                return df.Select(a, b, a & b);
            });

        [Fact(DisplayName = "Array except function can be used to except 2 array columns")]
        public static async Task Case14() =>
            await DebugDataframe(s =>
            {
                var a = ArrayColumn.New<IntegerColumn>("a");
                var b = ArrayColumn.New<IntegerColumn>("b");
                var df = s.CreateDataFrameFromData(
                    new { a = new[] { 1, 2, 3, 4 }, b = new[] { 3, 4, 5, 6 } }
                );
                return df.Select(a, b, a.Except(b));
            });

        [Fact(DisplayName = "ExplodeOuter can be called on an array column with values")]
        public static async Task Case15a() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col.ExplodeOuter());
            });

        [Fact(DisplayName = "ExplodeOuter can be called on an empty array column")]
        public static async Task Case15b() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new int[] { } });
                return df.Select(col.ExplodeOuter());
            });

        [Fact(DisplayName = "Size can be called on an array column")]
        public static async Task Case16() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col, col.Length());
            });

        [Fact(DisplayName = "Min can be called on an array column")]
        public static async Task Case17() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col, col.Min());
            });

        [Fact(DisplayName = "Max can be called on an array column")]
        public static async Task Case18() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col, col.Max());
            });

        [Fact(DisplayName = "Shuffle can be called on an array column")]
        public static async Task Case19() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col, !col.Shuffle().EqNullSafe(col));
            });

        [Fact(DisplayName = "Reverse can be called on an array column")]
        public static async Task Case20() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3, 4, 5 } });
                return df.Select(col, col.Reverse());
            });

        [Fact(DisplayName = "Flatten can be called on an array column")]
        public static async Task Case21() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<ArrayColumn<IntegerColumn>>("test");
                var df = s.CreateDataFrameFromData(
                    new { test = new[] { new[] { 1, 2 }, new[] { 3, 4, 5 } } }
                );
                return df.Select(col, col.Flatten());
            });

        [Fact(DisplayName = "Sequence can be used to create an array column")]
        public static async Task Case22() =>
            await DebugDataframe(s => s.CreateEmptyFrame().Select(ArrayColumn.Range(1, 10, 2)));

        [Fact(DisplayName = "Repeat can be used to create an array column")]
        public static async Task Case23() =>
            await DebugDataframe(s => s.CreateEmptyFrame().Select(((StringColumn)"a").Repeat(6)));

        [Fact(DisplayName = "Filter can be called on an array column")]
        public static async Task Case24() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 1, 2, 3 } });
                return df.Select(col, col.Filter(x => x % 2 == 1));
            });

        [Fact(DisplayName = "Filter with index can be called on an array column")]
        public static async Task Case25() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(new { test = new[] { 0, 2, 3 } });
                return df.Select(col, col.Filter((x, i) => x > i));
            });

        [Fact(DisplayName = "Filter can be called on an array column to remove null")]
        public static async Task Case26() =>
            await DebugDataframe(s =>
            {
                var col = ArrayColumn.New<IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new { test = new int?[] { 0, null, 2, 3, null } }
                );
                return df.Select(col, col.Filter(x => x.IsNotNull()));
            });
    }
}
