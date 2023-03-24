using System.Linq;
using System.Threading.Tasks;
using BunsenBurner;
using FluentAssertions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkTest.NET;
using SparkTest.NET.Extensions;
using Xunit;
using Scenario = BunsenBurner.Scenario<BunsenBurner.Syntax.Aaa>;

namespace TypedSpark.NET.Test
{
    public abstract class BaseOperationTests<TColumn, TSparkType, TNativeType>
        where TColumn : TypedColumn<TColumn, TSparkType, TNativeType>
        where TSparkType : DataType
    {
        private readonly Scenario.Arranged<SparkSession> _arrangedSession =
            SparkSessionFactory.DefaultSession.ArrangeData();

        protected abstract TColumn Create(string name, Column? column = default);

        protected abstract TNativeType ExampleValue1();
        protected abstract TNativeType ExampleValue2();

        [Fact(DisplayName = "== operator can be applied to a column and literal")]
        public async Task Case1() =>
            await _arrangedSession
                .Act(session =>
                {
                    var col = Create("A");
                    var df = session.CreateDataFrameFromData(
                        new { A = ExampleValue1() },
                        new { A = ExampleValue2() },
                        new { A = ExampleValue1() }
                    );
                    return df.Select((Column)col).Where((Column)(col == ExampleValue1())).Collect();
                })
                .Assert(rows =>
                {
                    rows.Should().HaveCount(2);
                    rows.SelectMany(x => x.Values)
                        .Should()
                        .ContainInOrder(ExampleValue1(), ExampleValue1());
                });

        [Fact(DisplayName = "== operator can be applied to a column and another column")]
        public async Task Case2() =>
            await _arrangedSession
                .Act(session =>
                {
                    var a = Create("A");
                    var b = Create("B");
                    var df = session.CreateDataFrameFromData(
                        new { A = ExampleValue1(), B = ExampleValue2() },
                        new { A = ExampleValue2(), B = ExampleValue2() },
                        new { A = ExampleValue1(), B = ExampleValue1() }
                    );
                    return df.Select((Column)a, (Column)b).Where((Column)(a == b)).Collect();
                })
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
            await _arrangedSession
                .Act(session =>
                {
                    var col = Create("A");
                    var df = session.CreateDataFrameFromData(
                        new { A = ExampleValue1() },
                        new { A = ExampleValue2() },
                        new { A = ExampleValue1() }
                    );
                    return df.Select((Column)col).Where((Column)(col != ExampleValue1())).Collect();
                })
                .Assert(rows =>
                {
                    rows.Should().HaveCount(1);
                    rows.SelectMany(x => x.Values).Should().ContainInOrder(ExampleValue2());
                });

        [Fact(DisplayName = "!= operator can be applied to a column and another column")]
        public async Task Case4() =>
            await _arrangedSession
                .Act(session =>
                {
                    var a = Create("A");
                    var b = Create("B");
                    var df = session.CreateDataFrameFromData(
                        new { A = ExampleValue1(), B = ExampleValue2() },
                        new { A = ExampleValue2(), B = ExampleValue2() },
                        new { A = ExampleValue1(), B = ExampleValue1() }
                    );
                    return df.Select((Column)a, (Column)b).Where((Column)(a != b)).Collect();
                })
                .Assert(rows =>
                {
                    rows.Should().HaveCount(1);
                    rows.SelectMany(x => x.Values)
                        .Should()
                        .ContainInOrder(ExampleValue1(), ExampleValue2());
                });

        [Fact(DisplayName = "EqNullSafe can be used against another column")]
        public async Task Case5() =>
            await _arrangedSession
                .Act(session =>
                {
                    var a = Create("A");
                    var df = session.CreateDataFrameFromData(new { A = ExampleValue1() });
                    return df.Select((Column)a).Where((Column)a.EqNullSafe(a)).Collect();
                })
                .Assert(rows => rows.Should().HaveCount(1));

        // [Fact(DisplayName = "When and Otherwise can be used")]
        // public async Task Case6() =>
        //     await _arrangedSession
        //         .Act(session =>
        //         {
        //             var a = Create("A");
        //             var lit = Create("B", Microsoft.Spark.Sql.Functions.Lit(ExampleValue1()));
        //             var df = session.CreateDataFrameFromData(
        //                 new { A = ExampleValue1() },
        //                 new { A = ExampleValue2() }
        //             );
        //             return df.Select(
        //                     (Column)a,
        //                     lit.CastToObject(),
        //                     a.When(a == lit, lit).Otherwise(lit).CastToObject().As("C")
        //                 )
        //                 .Where((Column)a.EqNullSafe(a))
        //                 .Collect();
        //         })
        //         .Assert(rows => rows.Should().HaveCount(1));
    }
}
