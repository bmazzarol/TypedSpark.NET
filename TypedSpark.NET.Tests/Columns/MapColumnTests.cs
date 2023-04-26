using System.Collections.Generic;
using System.Threading.Tasks;
using BunsenBurner;
using BunsenBurner.Verify.Xunit;
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
            await ArrangeUsingSpark(
                    s =>
                        s.CreateEmptyFrame()
                            .Select(
                                MapColumn.New(
                                    ArrayColumn.Range(1, 10),
                                    ((StringColumn)"a").Repeat(10)
                                )
                            )
                )
                .Act(df => df.Debug())
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Index by key can be used on a map column")]
        public static async Task Case2() =>
            await ArrangeUsingSpark(s =>
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
                })
                .Act(df => df.Debug())
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Map column can be created from key value pairs")]
        public static async Task Case3() =>
            await ArrangeUsingSpark(
                    s =>
                        s.CreateEmptyFrame()
                            .Select(
                                MapColumn.New(
                                    KeyValuePair.Create((IntegerColumn)1, (StringColumn)"a"),
                                    KeyValuePair.Create((IntegerColumn)2, (StringColumn)"b"),
                                    KeyValuePair.Create((IntegerColumn)3, (StringColumn)"c")
                                )
                            )
                )
                .Act(df => df.Debug())
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Size can be called on a map column")]
        public static async Task Case4() =>
            await ArrangeUsingSpark(s =>
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
                })
                .Act(df => df.Debug())
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Keys, values and entries can be called on a map column")]
        public static async Task Case5() =>
            await ArrangeUsingSpark(s =>
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
                        col.Entries().Get(c => c.Key),
                        col.Entries().Get(c => c.Value)
                    );
                })
                .Act(df => df.Debug())
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Concat can be called on a map column")]
        public static async Task Case6() =>
            await ArrangeUsingSpark(s =>
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
                })
                .Act(df => df.Debug())
                .AssertResultIsUnchanged();
    }
}
