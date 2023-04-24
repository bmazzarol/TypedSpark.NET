using System;
using System.Threading.Tasks;
using BunsenBurner;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using VerifyXunit;
using Xunit;
using static TypedSpark.NET.Tests.SparkTestExtensions;

namespace TypedSpark.NET.Tests
{
    [UsesVerify]
    public static class TypedDataFrameTests
    {
        public class TestSchema : TypedSchema<TestSchema>
        {
            public StringColumn A { get; private set; } = string.Empty;
            public IntegerColumn B { get; private set; } = default(int);
            public DateColumn C { get; private set; } = DateTime.Now;

            public TestSchema(string? alias) : base(alias) { }

            public TestSchema() : base(default) { }
        }

        [Fact(DisplayName = "Select can be used on a typed data frame")]
        public static async Task Case1() =>
            await SnapshotDataframe(
                s =>
                    s.CreateDataFrameFromData(
                            new { A = "1", B = 1, C = DateTime.MinValue },
                            new { A = "2", B = 2, C = DateTime.MaxValue }
                        )
                        .AsTyped<TestSchema>()
                        .Select(x => new { x.A })
            );

        [Fact(DisplayName = "Where can be used on a typed data frame")]
        public static async Task Case2() =>
            await SnapshotDataframe(
                s =>
                    s.CreateDataFrameFromData(
                            new { A = "1", B = 1, C = DateTime.MinValue },
                            new { A = "2", B = 2, C = DateTime.MaxValue }
                        )
                        .AsTyped<TestSchema>()
                        .Where(x => x.A == "2" & x.B > 1)
            );

        [Fact(DisplayName = "SelectMany can be used on a typed data frame")]
        public static async Task Case3() =>
            await SnapshotDataframe(s =>
            {
                var tdf = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>();
                return tdf.Alias("a").SelectMany(a => tdf.Alias("b"));
            });

        [Fact(DisplayName = "SelectMany with projection can be used on a typed data frame")]
        public static async Task Case3A() =>
            await SnapshotDataframe(s =>
            {
                var tdf = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>();
                return from a in tdf.Alias("a")
                from b in tdf.Alias("b")
                select new { A1 = a.A, A2 = b.A, C = a.B + b.B };
            });

        [Fact(DisplayName = "Union can be used on typed data frames")]
        public static async Task Case4() =>
            await SnapshotDataframe(s =>
            {
                var tdf = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>();
                return tdf.Alias("a") & tdf.Alias("b");
            });

        [Fact(DisplayName = "Intersect can be used on typed data frames")]
        public static async Task Case5() =>
            await SnapshotDataframe(s =>
            {
                var a = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>();
                var b = s.CreateDataFrameFromData(
                        new { A = "2", B = 2, C = DateTime.MaxValue },
                        new { A = "3", B = 3, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>();
                return a | b;
            });

        [Fact(DisplayName = "Intersect all can be used on typed data frames")]
        public static async Task Case6() =>
            await SnapshotDataframe(s =>
            {
                var a = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>();
                var b = s.CreateDataFrameFromData(
                        new { A = "2", B = 2, C = DateTime.MaxValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue },
                        new { A = "3", B = 3, C = DateTime.MaxValue },
                        new { A = "3", B = 3, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>();
                return a.IntersectAll(b);
            });

        [Fact(DisplayName = "Inner join can be used on typed data frames")]
        public static async Task Case7() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.InnerJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Cross join can be used on typed data frames")]
        public static async Task Case8() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.CrossJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Sort can be used on typed data frames")]
        public static async Task Case9() =>
            await SnapshotDataframe(
                s =>
                    s.CreateDataFrameFromData(
                            new { A = "1", B = 1, C = DateTime.MinValue },
                            new { A = "2", B = 2, C = DateTime.MaxValue },
                            new { A = "2", B = 3, C = DateTime.MaxValue }
                        )
                        .AsTyped<TestSchema>()
                        .OrderBy(x => new { A = x.A.Desc(), x.B })
            );

        [Fact(DisplayName = "Limit can be used on typed data frames")]
        public static async Task Case10() =>
            await SnapshotDataframe(
                s =>
                    s.CreateDataFrameFromData(
                            new { A = "1", B = 1, C = DateTime.MinValue },
                            new { A = "2", B = 2, C = DateTime.MaxValue },
                            new { A = "2", B = 3, C = DateTime.MaxValue }
                        )
                        .AsTyped<TestSchema>()
                        .Limit(1)
            );

        [Fact(DisplayName = "Outer join can be used on typed data frames")]
        public static async Task Case11() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.OuterJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Full join can be used on typed data frames")]
        public static async Task Case12() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.FullJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Full outer join can be used on typed data frames")]
        public static async Task Case13() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.FullOuterJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Left join can be used on typed data frames")]
        public static async Task Case14() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.LeftJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Left outer join can be used on typed data frames")]
        public static async Task Case15() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.LeftOuterJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Right join can be used on typed data frames")]
        public static async Task Case16() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.RightJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });

        [Fact(DisplayName = "Right outer join can be used on typed data frames")]
        public static async Task Case17() =>
            await SnapshotDataframe(s =>
            {
                var df1 = s.CreateDataFrameFromData(
                        new { A = "1", B = 1, C = DateTime.MinValue },
                        new { A = "2", B = 2, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("a");
                var df2 = s.CreateDataFrameFromData(
                        new { A = "3", B = 2, C = DateTime.MaxValue },
                        new { A = "4", B = 1, C = DateTime.MaxValue }
                    )
                    .AsTyped<TestSchema>()
                    .Alias("b");
                return df1.RightOuterJoin(
                    df2,
                    (a, b) => a.B == b.B,
                    (a, b) => new { A1 = a.A, A2 = b.A }
                );
            });
    }
}
