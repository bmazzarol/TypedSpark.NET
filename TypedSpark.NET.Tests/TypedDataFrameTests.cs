using System;
using System.Linq;
using System.Threading.Tasks;
using BunsenBurner;
using FluentAssertions;
using Microsoft.Spark.Sql.Types;
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
            await ArrangeUsingSpark(
                    s =>
                        s.CreateDataFrameFromData(
                                new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                            )
                            .AsTyped<TestSchema>()
                )
                .Act(tdf => tdf.Select(x => new { x.A }))
                .Assert(df =>
                {
                    var result = df.DataFrame.Collect().ToList();
                    result.Should().HaveCount(2);

                    var first = result.First();
                    first.Schema.Fields.Should().HaveCount(1);
                    first.Schema.Fields[0].Name.Should().Be("A");
                    first.Schema.Fields[0].DataType.TypeName.Should().Be("string");
                    first.Values[0].Should().Be("1");

                    var second = result.Last();
                    second.Schema.Fields.Should().HaveCount(1);
                    second.Schema.Fields[0].Name.Should().Be("A");
                    second.Schema.Fields[0].DataType.TypeName.Should().Be("string");
                    second.Values[0].Should().Be("2");
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Where can be used on a typed data frame")]
        public static async Task Case2() =>
            await ArrangeUsingSpark(
                    s =>
                        s.CreateDataFrameFromData(
                                new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                            )
                            .AsTyped<TestSchema>()
                )
                .Act(tdf => tdf.Where(x => x.A == "2" & x.B > 1))
                .Assert(df =>
                {
                    var result = df.DataFrame.Collect().ToList();
                    result.Should().HaveCount(1);

                    var first = result.First();
                    first.Schema.Fields.Should().HaveCount(3);
                    first.Schema.Fields[0].Name.Should().Be("A");
                    first.Schema.Fields[0].DataType.TypeName.Should().Be("string");
                    first.Schema.Fields[1].Name.Should().Be("B");
                    first.Schema.Fields[1].DataType.TypeName.Should().Be("integer");
                    first.Values[0].Should().Be("2");
                    first.Values[1].Should().Be(2);
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "SelectMany can be used on a typed data frame")]
        public static async Task Case3() =>
            await ArrangeUsingSpark(
                    s =>
                        s.CreateDataFrameFromData(
                                new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                            )
                            .AsTyped<TestSchema>()
                )
                .Act(tdf => tdf.Alias("a").SelectMany(a => tdf.Alias("b")))
                .Assert(df =>
                {
                    var result = df.DataFrame.Count();
                    result.Should().Be(4);
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "SelectMany with projection can be used on a typed data frame")]
        public static async Task Case3A() =>
            await ArrangeUsingSpark(
                    s =>
                        s.CreateDataFrameFromData(
                                new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                            )
                            .AsTyped<TestSchema>()
                )
                .Act(
                    tdf =>
                        from a in tdf.Alias("a")
                        from b in tdf.Alias("b")
                        select new { A1 = a.A, A2 = b.A, C = a.B + b.B }
                )
                .Assert(df =>
                {
                    var result = df.DataFrame.Count();
                    result.Should().Be(4);
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Union can be used on typed data frames")]
        public static async Task Case4() =>
            await ArrangeUsingSpark(
                    s =>
                        s.CreateDataFrameFromData(
                                new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                            )
                            .AsTyped<TestSchema>()
                )
                .Act(tdf => tdf.Alias("a") & tdf.Alias("b"))
                .Assert(df =>
                {
                    var result = df.DataFrame.Count();
                    result.Should().Be(4);
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Intersect can be used on typed data frames")]
        public static async Task Case5() =>
            await ArrangeUsingSpark(
                    s =>
                        (
                            a: s.CreateDataFrameFromData(
                                    new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>(),
                            b: s.CreateDataFrameFromData(
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) },
                                    new { A = "3", B = 3, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>()
                        )
                )
                .Act(t => t.a | t.b)
                .Assert(df =>
                {
                    var result = df.DataFrame.Count();
                    result.Should().Be(1);
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Intersect all can be used on typed data frames")]
        public static async Task Case6() =>
            await ArrangeUsingSpark(
                    s =>
                        (
                            a: s.CreateDataFrameFromData(
                                    new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) },
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>(),
                            b: s.CreateDataFrameFromData(
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) },
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) },
                                    new { A = "3", B = 3, C = new Date(DateTime.MaxValue) },
                                    new { A = "3", B = 3, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>()
                        )
                )
                .Act(t => t.a.IntersectAll(t.b))
                .Assert(df =>
                {
                    var result = df.DataFrame.Count();
                    result.Should().Be(2);
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Inner join can be used on typed data frames")]
        public static async Task Case7() =>
            await ArrangeUsingSpark(
                    s =>
                        (
                            a: s.CreateDataFrameFromData(
                                    new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>()
                                .Alias("a"),
                            b: s.CreateDataFrameFromData(
                                    new { A = "3", B = 2, C = new Date(DateTime.MaxValue) },
                                    new { A = "4", B = 1, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>()
                                .Alias("b")
                        )
                )
                .Act(
                    t =>
                        t.a.InnerJoin(
                            t.b,
                            (a, b) => a.B == b.B,
                            (a, b) => new { A1 = a.A, A2 = b.A }
                        )
                )
                .Assert(df =>
                {
                    var result = df.DataFrame.Collect().ToList();
                    result.Should().HaveCount(2);
                    result.First().Values.Should().BeEquivalentTo(new[] { "1", "4" });
                    result.Last().Values.Should().BeEquivalentTo(new[] { "2", "3" });
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Cross join can be used on typed data frames")]
        public static async Task Case8() =>
            await ArrangeUsingSpark(
                    s =>
                        (
                            a: s.CreateDataFrameFromData(
                                    new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                    new { A = "2", B = 2, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>()
                                .Alias("a"),
                            b: s.CreateDataFrameFromData(
                                    new { A = "3", B = 2, C = new Date(DateTime.MaxValue) },
                                    new { A = "4", B = 1, C = new Date(DateTime.MaxValue) }
                                )
                                .AsTyped<TestSchema>()
                                .Alias("b")
                        )
                )
                .Act(
                    t =>
                        t.a.CrossJoin(
                            t.b,
                            (a, b) => a.B == b.B,
                            (a, b) => new { A1 = a.A, A2 = b.A }
                        )
                )
                .Assert(df =>
                {
                    var result = df.DataFrame.Collect().ToList();
                    result.Should().HaveCount(2);
                    result.First().Values.Should().BeEquivalentTo(new[] { "1", "4" });
                    result.Last().Values.Should().BeEquivalentTo(new[] { "2", "3" });
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Sort can be used on typed data frames")]
        public static async Task Case9() =>
            await ArrangeUsingSpark(
                    s =>
                        s.CreateDataFrameFromData(
                                new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                new { A = "2", B = 2, C = new Date(DateTime.MaxValue) },
                                new { A = "2", B = 3, C = new Date(DateTime.MaxValue) }
                            )
                            .AsTyped<TestSchema>()
                )
                .Act(tdf => tdf.OrderBy(x => new { A = x.A.Desc(), x.B }))
                .Assert(df =>
                {
                    var result = df.DataFrame.Collect().ToList();
                    result.Should().HaveCount(3);
                    result[0].Values
                        .Should()
                        .BeEquivalentTo(new object[] { "2", 2, new Date(DateTime.MaxValue) });
                    result[1].Values
                        .Should()
                        .BeEquivalentTo(new object[] { "2", 3, new Date(DateTime.MaxValue) });
                    result[2].Values
                        .Should()
                        .BeEquivalentTo(new object[] { "1", 1, new Date(DateTime.MinValue) });
                })
                .AndExplainPlanHasNotChanged();

        [Fact(DisplayName = "Limit can be used on typed data frames")]
        public static async Task Case10() =>
            await ArrangeUsingSpark(
                    s =>
                        s.CreateDataFrameFromData(
                                new { A = "1", B = 1, C = new Date(DateTime.MinValue) },
                                new { A = "2", B = 2, C = new Date(DateTime.MaxValue) },
                                new { A = "2", B = 3, C = new Date(DateTime.MaxValue) }
                            )
                            .AsTyped<TestSchema>()
                )
                .Act(tdf => tdf.Limit(1))
                .Assert(df =>
                {
                    var result = df.DataFrame.Collect().ToList();
                    result.Should().HaveCount(1);
                    result
                        .First()
                        .Values.Should()
                        .BeEquivalentTo(new object[] { "1", 1, new Date(DateTime.MinValue) });
                })
                .AndExplainPlanHasNotChanged();
    }
}
