using System.Linq;
using System.Threading.Tasks;
using BunsenBurner;
using FluentAssertions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkTest.NET.Extensions;
using Xunit;
using static TypedSpark.NET.Tests.SparkTestExtensions;

namespace TypedSpark.NET.Tests
{
    public abstract class BaseOperationTests<TColumn, TSparkType, TNativeType>
        where TColumn : TypedColumn<TColumn, TSparkType, TNativeType>, new()
        where TSparkType : DataType
    {
        protected abstract TColumn Create(string name, Column? column = default);

        protected abstract TNativeType ExampleValue1();
        protected abstract TNativeType ExampleValue2();

        protected abstract TColumn FromNative(TNativeType native);

        [Fact(DisplayName = "== operator can be applied to a column and literal")]
        public async Task Case1() =>
            await ArrangeUsingSpark(s =>
                {
                    var col = Create("A");
                    var df = s.CreateDataFrameFromData(
                        new { A = ExampleValue1() },
                        new { A = ExampleValue2() },
                        new { A = ExampleValue1() }
                    );
                    return df.Select((Column)col)
                        .Where((Column)(col == FromNative(ExampleValue1())));
                })
                .Act(df => df.Collect().ToList())
                .Assert(rows =>
                {
                    rows.Should().HaveCount(2);
                    rows.SelectMany(x => x.Values)
                        .Should()
                        .ContainInOrder(ExampleValue1(), ExampleValue1());
                });

        [Fact(DisplayName = "== operator can be applied to a column and another column")]
        public async Task Case2() =>
            await ArrangeUsingSpark(s =>
                {
                    var a = Create("A");
                    var b = Create("B");
                    var df = s.CreateDataFrameFromData(
                        new { A = ExampleValue1(), B = ExampleValue2() },
                        new { A = ExampleValue2(), B = ExampleValue2() },
                        new { A = ExampleValue1(), B = ExampleValue1() }
                    );
                    return df.Select((Column)a, (Column)b).Where((Column)(a == b));
                })
                .Act(df => df.Collect().ToList())
                .Assert(rows =>
                {
                    rows.Should().HaveCount(2);
                    rows.SelectMany(x => x.Values)
                        .Should()
                        .ContainInOrder(
                            ExampleValue2(),
                            ExampleValue2(),
                            ExampleValue1(),
                            ExampleValue1()
                        );
                });

        [Fact(DisplayName = "!= operator can be applied to a column and literal")]
        public async Task Case3() =>
            await ArrangeUsingSpark(s =>
                {
                    var col = Create("A");
                    var df = s.CreateDataFrameFromData(
                        new { A = ExampleValue1() },
                        new { A = ExampleValue2() },
                        new { A = ExampleValue1() }
                    );
                    return df.Select((Column)col)
                        .Where((Column)(col != FromNative(ExampleValue1())));
                })
                .Act(df => df.Collect().ToList())
                .Assert(rows =>
                {
                    rows.Should().HaveCount(1);
                    rows.SelectMany(x => x.Values).Should().ContainInOrder(ExampleValue2());
                });

        [Fact(DisplayName = "!= operator can be applied to a column and another column")]
        public async Task Case4() =>
            await ArrangeUsingSpark(s =>
                {
                    var a = Create("A");
                    var b = Create("B");
                    var df = s.CreateDataFrameFromData(
                        new { A = ExampleValue1(), B = ExampleValue2() },
                        new { A = ExampleValue2(), B = ExampleValue2() },
                        new { A = ExampleValue1(), B = ExampleValue1() }
                    );
                    return df.Select((Column)a, (Column)b).Where((Column)(a != b));
                })
                .Act(df => df.Collect().ToList())
                .Assert(rows =>
                {
                    rows.Should().HaveCount(1);
                    rows.SelectMany(x => x.Values)
                        .Should()
                        .ContainInOrder(ExampleValue1(), ExampleValue2());
                });

        [Fact(DisplayName = "EqNullSafe can be used against another column")]
        public async Task Case5() =>
            await ArrangeUsingSpark(s =>
                {
                    var a = Create("A");
                    var df = s.CreateDataFrameFromData(new { A = ExampleValue1() });
                    return df.Select((Column)a).Where((Column)a.EqNullSafe(a));
                })
                .Act(df => df.Collect())
                .Assert(rows => rows.Should().HaveCount(1));
    }
}
