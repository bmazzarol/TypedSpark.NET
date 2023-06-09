using System.Threading.Tasks;
using BunsenBurner;
using BunsenBurner.Utility;
using FluentAssertions;
using TypedSpark.NET.Columns;
using VerifyXunit;
using Xunit;
using static TypedSpark.NET.Tests.SparkTestExtensions;

namespace TypedSpark.NET.Tests
{
    [UsesVerify]
    public static class TypedSchemaTests
    {
        private class EmptySchema : TypedSchema<EmptySchema>
        {
            public EmptySchema(string? alias)
                : base(alias) { }

            public EmptySchema()
                : base(default) { }
        }

        [Fact(DisplayName = "Empty schemas will fail with an exception")]
        public static async Task Case1() =>
            await string.Empty
                .ArrangeData()
                .Act(_ => new EmptySchema())
                .AssertFailsWith(
                    e => e.Message.Should().Be("No properties have been defined on the schema")
                );

        private class NonColumnProperty : TypedSchema<NonColumnProperty>
        {
            public string A { get; private set; } = string.Empty;

            public NonColumnProperty(string? alias)
                : base(alias) { }

            public NonColumnProperty()
                : base(default) { }
        }

        [Fact(DisplayName = "Schemas with non TypedColumn properties will fail with an exception")]
        public static async Task Case2() =>
            await string.Empty
                .ArrangeData()
                .Act(_ => new NonColumnProperty())
                .AssertFailsWith(
                    e =>
                        e.Message
                            .Should()
                            .Be(
                                "All properties must be a TypedColumn with a public getter and private setter"
                            )
                );

        private class PublicSetter : TypedSchema<PublicSetter>
        {
            public StringColumn A { get; set; } = default!;

            public PublicSetter(string? alias)
                : base(alias) { }

            public PublicSetter()
                : base(default) { }
        }

        [Fact(
            DisplayName = "Schemas with TypedColumn property with a public setter will fail with an exception"
        )]
        public static async Task Case3() =>
            await string.Empty
                .ArrangeData()
                .Act(_ => new PublicSetter())
                .AssertFailsWith(
                    e =>
                        e.Message
                            .Should()
                            .Be(
                                "All properties must be a TypedColumn with a public getter and private setter"
                            )
                );

        private class ValidSchema : TypedSchema<ValidSchema>
        {
            public StringColumn A { get; private set; } = default!;
            public IntegerColumn B { get; private set; } = default!;

            public ValidSchema(string? alias)
                : base(alias) { }

            public ValidSchema()
                : base(default) { }
        }

        [Fact(DisplayName = "Valid schema column takes its name from the property name")]
        public static async Task Case4() =>
            await ArrangeUsingSpark(_ => new ValidSchema())
                .Act(_ => _)
                .Assert(s =>
                {
                    s.A.ToString().Should().Be("A");
                    s.B.ToString().Should().Be("B");
                })
                .And(s => Verifier.Verify(s.Type.Json).UseDirectory("__snapshots__"));

        private class InvalidFieldSchema : TypedSchema<InvalidFieldSchema>
        {
#pragma warning disable CS0414
            public readonly StringColumn InvalidField = null!;
#pragma warning restore CS0414

            public InvalidFieldSchema(string? alias)
                : base(alias) { }

            public InvalidFieldSchema()
                : base(default) { }
        }

        [Fact(DisplayName = "Fields are not supported on a typed schema")]
        public static async Task Case5() =>
            await string.Empty
                .ArrangeData()
                .Act(_ => new InvalidFieldSchema())
                .AssertFailsWith(
                    e => e.Message.Should().Be("Only properties are supported on a schema")
                );

        private class InvalidFieldSchema2 : TypedSchema<InvalidFieldSchema2>
        {
            public StringColumn A { get; private set; } = default!;
            public IntegerColumn B { get; private set; } = default!;

            public InvalidFieldSchema2(StringColumn a, string? alias = default)
                : base(alias, new[] { a }) { }

            public InvalidFieldSchema2()
                : base(default) { }
        }

        [Fact(DisplayName = "Provided columns need to match number and type of columns")]
        public static async Task Case6() =>
            await ArrangeUsingSpark(ManualDisposal.New)
                .Act(_ => new InvalidFieldSchema2(StringColumn.New("a")))
                .AssertFailsWith(
                    e =>
                        e.Message
                            .Should()
                            .Be(
                                "The number of columns provided 1 need to match the order and number of properties on the schema which is 2 (Parameter 'columns')"
                            )
                );
    }
}
