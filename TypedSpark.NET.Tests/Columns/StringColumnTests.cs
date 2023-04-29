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
    public static class StringColumnTests
    {
        [Fact(DisplayName = "Sentences can be called on a string column")]
        public static async Task Case1() =>
            await DebugDataframe(s =>
            {
                StringColumn sentence =
                    "This is a test. It should be split into sentences and words.";
                return s.CreateEmptyFrame()
                    .Select(
                        sentence.As("Sentence"),
                        sentence.Sentences(),
                        sentence.Sentences("en", "au")
                    );
            });
    }
}
