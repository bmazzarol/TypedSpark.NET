using System.Threading.Tasks;
using BunsenBurner;
using FluentAssertions;
using Xunit;
using static Microsoft.Spark.Sql.Functions;
using static TypedSpark.NET.Tests.SparkTestExtensions;

namespace TypedSpark.NET.Tests.Columns
{
    public static class ColumnExtensionsTests
    {
        [Fact(DisplayName = "Column with a name as its expression can be aliased")]
        public static async Task Case1() =>
            await ArrangeUsingSpark(
                    s => Column("SomeName").ApplyAlias("a", new[] { "SomeName", "SomeOtherName" })
                )
                .Act(c => c.ToString())
                .Assert(r => r.Should().Be("a.SomeName"));

        [Fact(DisplayName = "Aliased Column with a name as its expression can be aliased")]
        public static async Task Case2() =>
            await ArrangeUsingSpark(
                    s =>
                        Column("SomeName AS b")
                            .ApplyAlias("a", new[] { "someName", "SomeOtherName" })
                )
                .Act(c => c.ToString())
                .Assert(r => r.Should().Be("a.SomeName AS b"));

        [Fact(
            DisplayName = "Column with an existing aliased name as its expression is not re-aliased"
        )]
        public static async Task Case3() =>
            await ArrangeUsingSpark(
                    s => Column("a.SomeName").ApplyAlias("b", new[] { "someName", "SomeOtherName" })
                )
                .Act(c => c.ToString())
                .Assert(r => r.Should().Be("a.SomeName"));

        [Fact(DisplayName = "Column with a complex expression aliases all listed names")]
        public static async Task Case4() =>
            await ArrangeUsingSpark(
                    s =>
                        Column("SomeName = b.SomeName and NotListedName != SomeOtherName")
                            .ApplyAlias("a", new[] { "someName", "someOtherName" })
                )
                .Act(c => c.ToString())
                .Assert(
                    r =>
                        r.Should()
                            .Be("a.SomeName = b.SomeName and NotListedName != a.SomeOtherName")
                );
    }
}
