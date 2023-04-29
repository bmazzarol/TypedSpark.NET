using System;
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
    public static class StructColumnTests
    {
        sealed class Schema1 : TypedSchema<Schema1>
        {
            public StringColumn A { get; private set; } = string.Empty;
            public IntegerColumn B { get; private set; } = default(int);
            public DateColumn C { get; private set; } = DateTime.Now;

            public Schema1(string? alias)
                : base(alias) { }

            public Schema1()
                : base(default) { }
        }

        sealed class Schema2 : TypedSchema<Schema1>
        {
            public DateColumn C { get; private set; } = DateTime.Now;
            public LongColumn D { get; private set; } = default(int);

            public Schema2(string? alias)
                : base(alias) { }

            public Schema2()
                : base(default) { }
        }

        [Fact(DisplayName = "Struct column can be migrated")]
        public static async Task Case1() =>
            await DebugDataframe(s =>
            {
                var col = StructColumn.New<Schema1>("test");
                return s.CreateDataFrameFromData(
                        new
                        {
                            D = 100L,
                            test = new
                            {
                                A = "a",
                                B = 1,
                                C = DateTime.MinValue
                            }
                        }
                    )
                    .Select(col, col.Migrate<Schema2>());
            });

        [Fact(DisplayName = "Struct column can be created from columns")]
        public static async Task Case2() =>
            await DebugDataframe(
                s =>
                    s.CreateDataFrameFromData(
                            new
                            {
                                A = "a",
                                B = 1,
                                C = DateTime.MinValue,
                                D = 100L
                            }
                        )
                        .Select(
                            StringColumn.New("A"),
                            IntegerColumn.New("B"),
                            DateColumn.New("C"),
                            LongColumn.New("D"),
                            StructColumn.FromColumns<Schema1>(),
                            StructColumn.FromColumns<Schema2>()
                        )
            );
    }
}
