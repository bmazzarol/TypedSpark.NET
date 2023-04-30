using System.Collections.Generic;
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
    public static class MapColumnTests
    {
        [Fact(DisplayName = "Map column can be created from arrays")]
        public static async Task Case1() =>
            await DebugDataframe(
                s =>
                    s.CreateEmptyFrame()
                        .Select(
                            MapColumn.New(ArrayColumn.Range(1, 10), ((StringColumn)"a").Repeat(10))
                        )
            );

        [Fact(DisplayName = "Index by key can be used on a map column")]
        public static async Task Case2() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<StringColumn, IntegerColumn>("a");
                return s.CreateDataFrameFromData(
                        new
                        {
                            a = new Dictionary<string, int>
                            {
                                ["a"] = 1,
                                ["b"] = 2,
                                ["c"] = 3,
                                ["d"] = 4,
                                ["e"] = 5,
                            }
                        }
                    )
                    .Select(col, col["a"], col["z"]);
            });

        [Fact(DisplayName = "Map column can be created from key value pairs")]
        public static async Task Case3() =>
            await DebugDataframe(
                s =>
                    s.CreateEmptyFrame()
                        .Select(
                            MapColumn.New(
                                KeyValuePair.Create((IntegerColumn)1, (StringColumn)"a"),
                                KeyValuePair.Create((IntegerColumn)2, (StringColumn)"b"),
                                KeyValuePair.Create((IntegerColumn)3, (StringColumn)"c")
                            )
                        )
            );

        [Fact(DisplayName = "Size can be called on a map column")]
        public static async Task Case4() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<IntegerColumn, StringColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        test = new Dictionary<string, int>
                        {
                            ["a"] = 1,
                            ["b"] = 2,
                            ["c"] = 3,
                            ["d"] = 4,
                            ["e"] = 5,
                        }
                    }
                );
                return df.Select(col, col.Length());
            });

        [Fact(DisplayName = "Keys, values and entries can be called on a map column")]
        public static async Task Case5() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<IntegerColumn, StringColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        test = new Dictionary<string, int>
                        {
                            ["a"] = 1,
                            ["b"] = 2,
                            ["c"] = 3,
                            ["d"] = 4,
                            ["e"] = 5,
                        }
                    }
                );
                return df.Select(
                    col,
                    col.Keys(),
                    col.Values(),
                    col.Entries(),
                    col.Entries().Get(c => c.Item1),
                    col.Entries().Get(c => c.Item2)
                );
            });

        [Fact(DisplayName = "Concat can be called on a map column")]
        public static async Task Case6() =>
            await DebugDataframe(s =>
            {
                var a = MapColumn.New<IntegerColumn, StringColumn>("a");
                var b = MapColumn.New<IntegerColumn, StringColumn>("b");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        a = new Dictionary<string, int>
                        {
                            ["a"] = 1,
                            ["b"] = 2,
                            ["c"] = 3,
                        },
                        b = new Dictionary<string, int> { ["d"] = 4, ["e"] = 5, }
                    }
                );
                return df.Select(a, b, a & b);
            });

        [Fact(DisplayName = "Filter can be called on a map column")]
        public static async Task Case7() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<StringColumn, IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        test = new Dictionary<string, int>
                        {
                            ["1"] = 0,
                            ["2"] = 2,
                            ["3"] = -1,
                        }
                    }
                );
                return df.Select(col, col.Filter((k, v) => k > v));
            });

        [Fact(DisplayName = "Explode can be called on a map column")]
        public static async Task Case8() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<StringColumn, IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        test = new Dictionary<string, int>
                        {
                            ["1"] = 0,
                            ["2"] = 2,
                            ["3"] = -1,
                        }
                    }
                );
                return df.Select(col.Explode(out var k, out var v), k, v);
            });

        [Fact(DisplayName = "Explode outer can be called on a map column")]
        public static async Task Case9() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<StringColumn, IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        test = new Dictionary<string, int>
                        {
                            ["1"] = 0,
                            ["2"] = 2,
                            ["3"] = -1,
                        }
                    }
                );
                return df.Select(col.ExplodeOuter(out var k, out var v), k, v);
            });

        [Fact(DisplayName = "PosExplode can be called on a map column")]
        public static async Task Case10() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<StringColumn, IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        test = new Dictionary<string, int>
                        {
                            ["1"] = 0,
                            ["2"] = 2,
                            ["3"] = -1,
                        }
                    }
                );
                return df.Select(col.PosExplode(out var i, out var k, out var v), i, k, v);
            });

        [Fact(DisplayName = "PosExplode outer can be called on a map column")]
        public static async Task Case11() =>
            await DebugDataframe(s =>
            {
                var col = MapColumn.New<StringColumn, IntegerColumn>("test");
                var df = s.CreateDataFrameFromData(
                    new
                    {
                        test = new Dictionary<string, int>
                        {
                            ["1"] = 0,
                            ["2"] = 2,
                            ["3"] = -1,
                        }
                    }
                );
                return df.Select(col.PosExplodeOuter(out var i, out var k, out var v), i, k, v);
            });
    }
}
