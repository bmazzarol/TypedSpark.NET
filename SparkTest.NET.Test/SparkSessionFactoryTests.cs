using System;
using System.Linq;
using System.Threading.Tasks;
using BunsenBurner;
using BunsenBurner.Verify.Xunit;
using FluentAssertions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkTest.NET.Extensions;
using VerifyXunit;
using Xunit;
using Scenario = BunsenBurner.Scenario<BunsenBurner.Syntax.Aaa>;

namespace SparkTest.NET.Test
{
    [UsesVerify]
    public static class SparkSessionFactoryTests
    {
        private static readonly Scenario.Arranged<SparkSession> ArrangedSession =
            SparkSessionFactory.DefaultSession.ArrangeData();

        [Fact(DisplayName = "A spark session can be returned and used to query")]
        public static async Task Case1() =>
            await ArrangedSession
                .Act(session => session.CreateDataFrame(Enumerable.Range(1, 10)).Collect())
                .Assert(rows => rows.Count() == 10);

        [Fact(DisplayName = "A spark data frame can be created from a custom type")]
        public static async Task Case2() =>
            await ArrangedSession
                .Act(
                    session =>
                        session.CreateDataFrameFromData(new { A = 1, B = "2", C = 3.0 }).Collect()
                )
                .Assert(rows => rows.Count() == 1)
                .And(
                    rows =>
                        rows.First().Values.Should().BeEquivalentTo(new object[] { 1, "2", 3.0 })
                );

        [Fact(DisplayName = "All the spark types are supported from dotnet custom types")]
        public static async Task Case3() =>
            await ArrangedSession
                .Act(
                    session =>
                        session
                            .CreateDataFrameFromData(
                                new
                                {
                                    Bool = true,
                                    Int = 1,
                                    Long = (long)1,
                                    Double = 0.0,
                                    Date = new Date(DateTime.Now),
                                    Timestamp = new Timestamp(DateTime.Now),
                                    String = ""
                                }
                            )
                            .Collect()
                            .First()
                            .Schema.Json
                )
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Nested objects are supported")]
        public static async Task Case4() =>
            await new
            {
                Id = 1,
                Details = new
                {
                    Name = "Ben",
                    Age = 20,
                    NumericTypes = new
                    {
                        Byte = (byte)0x0,
                        Short = (short)1,
                        Decimal = (decimal)0.0,
                        Float = (float)0.0
                    }
                }
            }
                .ArrangeData()
                .Act(data => data.AsStructType(false).Json)
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Objects that cannot be serialized from .NET to Java throw")]
        public static async Task Case5() =>
            await new { Guid = Guid.NewGuid() }
                .ArrangeData()
                .Act(data => data.AsStructType(true))
                .AssertFailsWith(
                    (NotSupportedException e) =>
                        e.Message
                            .Should()
                            .Be(
                                "System.Guid is not supported in Spark or cannot be serialized from .NET to Java at this time"
                            )
                );

        [Fact(DisplayName = "Nullable objects are supported")]
        public static async Task Case6() =>
            await new { NullableInt = (int?)null }
                .ArrangeData()
                .Act(data => data.AsStructType(true).Json)
                .AssertResultIsUnchanged();

        [Fact(DisplayName = "Objects that are not supported in Spark throw")]
        public static async Task Case7() =>
            await new { Guid = Guid.NewGuid() }
                .ArrangeData()
                .Act(data => data.AsStructType(false))
                .AssertFailsWith(
                    (NotSupportedException e) =>
                        e.Message.Should().Be("System.Guid is not supported in Spark")
                );

        [Fact(DisplayName = "Empty data frame returns a single empty row")]
        public static async Task Case8() =>
            await ArrangedSession
                .Act(session => session.CreateEmptyFrame().Collect())
                .Assert(rows =>
                {
                    rows.Should().HaveCount(1);
                    rows.SelectMany(x => x.Values).Should().BeEmpty();
                });
    }
}
