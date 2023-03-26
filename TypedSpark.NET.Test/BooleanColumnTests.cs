using System.Linq;
using System.Threading.Tasks;
using BunsenBurner;
using FluentAssertions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkTest.NET;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Extensions;
using TypedSpark.NET.Generators;
using Xunit;
using Scenario = BunsenBurner.Scenario<BunsenBurner.Syntax.Aaa>;
using Fn = TypedSpark.NET.Functions;
using B = TypedSpark.NET.Columns.BooleanColumn;

namespace TypedSpark.NET.Test
{
    public static class BooleanColumnTests
    {
        private static readonly Scenario.Arranged<SparkSession> ArrangedSession =
            SparkSessionFactory.DefaultSession.ArrangeData();

        [Fact(DisplayName = "! operator can be applied to the boolean column")]
        public static async Task Case1() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col = B.New("Bool");
                    var df = session.CreateDataFrameFromData(
                        new { Bool = false },
                        new { Bool = false },
                        new { Bool = true }
                    );
                    return df.Select((Column)!col).Collect();
                })
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values).Should().ContainInOrder(true, true, false)
                );

        [Fact(DisplayName = "OR operator can be applied to the boolean columns")]
        public static async Task Case2() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var col2 = B.New("Bool2");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false, Bool2 = false },
                        new { Bool1 = false, Bool2 = true },
                        new { Bool1 = true, Bool2 = false },
                        new { Bool1 = true, Bool2 = true }
                    );
                    return df.Select((Column)(col1 | col2)).Collect();
                })
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder(false, true, true, true)
                );

        [Fact(DisplayName = "AND operator can be applied to the boolean columns")]
        public static async Task Case3() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var col2 = B.New("Bool2");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false, Bool2 = false },
                        new { Bool1 = false, Bool2 = true },
                        new { Bool1 = true, Bool2 = false },
                        new { Bool1 = true, Bool2 = true }
                    );
                    return df.Select((Column)(col1 & col2)).Collect();
                })
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder(false, false, false, true)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a StringColumn")]
        public static async Task Case4() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(new { Bool1 = false });
                    return df.Select((Column)Fn.Length(col1)).Collect();
                })
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(5));

        [Fact(DisplayName = "BooleanColumn can be cast to a ByteColumn")]
        public static async Task Case5() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToByte()).Collect();
                })
                .Assert(
                    rows => rows.SelectMany(x => x.Values).Should().ContainInOrder((byte)0, (byte)1)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a ByteColumn")]
        public static async Task Case6() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToByte()).Collect();
                })
                .Assert(
                    rows => rows.SelectMany(x => x.Values).Should().ContainInOrder((byte)0, (byte)1)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a ShortColumn")]
        public static async Task Case7() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToShort()).Collect();
                })
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values).Should().ContainInOrder((short)0, (short)1)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a IntegerColumn")]
        public static async Task Case8() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToInteger()).Collect();
                })
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(0, 1));

        [Fact(DisplayName = "BooleanColumn can be cast to a LongColumn")]
        public static async Task Case9() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToLong()).Collect();
                })
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(0L, 1L));

        [Fact(DisplayName = "BooleanColumn can be cast to a FloatColumn")]
        public static async Task Case10() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToFloat()).Collect();
                })
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder((float)0.0, (float)1.0)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a DecimalColumn")]
        public static async Task Case11() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToDecimal()).Collect();
                })
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder((decimal)0.0, (decimal)1.0)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a DoubleColumn")]
        public static async Task Case12() =>
            await ArrangedSession
                .Act(session =>
                {
                    var col1 = B.New("Bool1");
                    var df = session.CreateDataFrameFromData(
                        new { Bool1 = false },
                        new { Bool1 = true }
                    );
                    return df.Select((Column)col1.CastToDouble()).Collect();
                })
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(0.0, 1.0));

        [Fact(DisplayName = "BooleanColumns can be created from literals")]
        public static async Task Case13() =>
            await ArrangedSession
                .Act(session =>
                {
                    B a = true;
                    B b = false;
                    var df = session.CreateEmptyFrame();
                    return df.Select((Column)a.As("A"), (Column)b.As("B")).Collect();
                })
                .Assert(
                    rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(true, false)
                );
    }

    public sealed class BooleanBaseOperations : BaseOperationTests<B, BooleanType, bool>
    {
        protected override B Create(string name, Column? column = default) => B.New(name, column);

        protected override bool ExampleValue1() => true;

        protected override bool ExampleValue2() => false;

        protected override B FromNative(bool native) => native;
    }

    [GenerateSchema]
    public partial class Test { }
}
