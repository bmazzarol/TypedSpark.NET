using System.Linq;
using System.Threading.Tasks;
using BunsenBurner;
using FluentAssertions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkTest.NET.Extensions;
using Xunit;
using static TypedSpark.NET.Tests.SparkTestExtensions;
using Fn = TypedSpark.NET.Functions;
using B = TypedSpark.NET.Columns.BooleanColumn;

namespace TypedSpark.NET.Tests
{
    public static class BooleanColumnTests
    {
        [Fact(DisplayName = "! operator can be applied to the boolean column")]
        public static async Task Case1() =>
            await ArrangeUsingSpark(s =>
                {
                    var col = B.New("Bool");
                    var df = s.CreateDataFrameFromData(
                        new { Bool = false },
                        new { Bool = false },
                        new { Bool = true }
                    );
                    return df.Select((Column)!col);
                })
                .Act(df => df.Collect())
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values).Should().ContainInOrder(true, true, false)
                );

        [Fact(DisplayName = "OR operator can be applied to the boolean columns")]
        public static async Task Case2() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var col2 = B.New("Bool2");
                    var df = s.CreateDataFrameFromData(
                        new { Bool1 = false, Bool2 = false },
                        new { Bool1 = false, Bool2 = true },
                        new { Bool1 = true, Bool2 = false },
                        new { Bool1 = true, Bool2 = true }
                    );
                    return df.Select((Column)(col1 | col2));
                })
                .Act(df => df.Collect())
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder(false, true, true, true)
                );

        [Fact(DisplayName = "AND operator can be applied to the boolean columns")]
        public static async Task Case3() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var col2 = B.New("Bool2");
                    var df = s.CreateDataFrameFromData(
                        new { Bool1 = false, Bool2 = false },
                        new { Bool1 = false, Bool2 = true },
                        new { Bool1 = true, Bool2 = false },
                        new { Bool1 = true, Bool2 = true }
                    );
                    return df.Select((Column)(col1 & col2));
                })
                .Act(df => df.Collect())
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder(false, false, false, true)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a StringColumn")]
        public static async Task Case4() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false });
                    return df.Select((Column)Fn.Length(col1));
                })
                .Act(df => df.Collect())
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(5));

        [Fact(DisplayName = "BooleanColumn can be cast to a ByteColumn")]
        public static async Task Case5() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToByte());
                })
                .Act(df => df.Collect())
                .Assert(
                    rows => rows.SelectMany(x => x.Values).Should().ContainInOrder((byte)0, (byte)1)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a ByteColumn")]
        public static async Task Case6() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToByte());
                })
                .Act(df => df.Collect())
                .Assert(
                    rows => rows.SelectMany(x => x.Values).Should().ContainInOrder((byte)0, (byte)1)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a ShortColumn")]
        public static async Task Case7() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToShort());
                })
                .Act(df => df.Collect())
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values).Should().ContainInOrder((short)0, (short)1)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a IntegerColumn")]
        public static async Task Case8() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToInteger());
                })
                .Act(df => df.Collect())
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(0, 1));

        [Fact(DisplayName = "BooleanColumn can be cast to a LongColumn")]
        public static async Task Case9() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToLong());
                })
                .Act(df => df.Collect())
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(0L, 1L));

        [Fact(DisplayName = "BooleanColumn can be cast to a FloatColumn")]
        public static async Task Case10() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToFloat());
                })
                .Act(df => df.Collect())
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder((float)0.0, (float)1.0)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a DecimalColumn")]
        public static async Task Case11() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToDecimal());
                })
                .Act(df => df.Collect())
                .Assert(
                    rows =>
                        rows.SelectMany(x => x.Values)
                            .Should()
                            .ContainInOrder((decimal)0.0, (decimal)1.0)
                );

        [Fact(DisplayName = "BooleanColumn can be cast to a DoubleColumn")]
        public static async Task Case12() =>
            await ArrangeUsingSpark(s =>
                {
                    var col1 = B.New("Bool1");
                    var df = s.CreateDataFrameFromData(new { Bool1 = false }, new { Bool1 = true });
                    return df.Select((Column)col1.CastToDouble());
                })
                .Act(df => df.Collect())
                .Assert(rows => rows.SelectMany(x => x.Values).Should().ContainInOrder(0.0, 1.0));

        [Fact(DisplayName = "BooleanColumns can be created from literals")]
        public static async Task Case13() =>
            await ArrangeUsingSpark(s =>
                {
                    B a = true;
                    B b = false;
                    var df = s.CreateEmptyFrame();
                    return df.Select((Column)a.As("A"), (Column)b.As("B"));
                })
                .Act(df => df.Collect())
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
}
