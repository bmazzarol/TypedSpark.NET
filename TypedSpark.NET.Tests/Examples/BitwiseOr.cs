using Docfx.ResultSnippets;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class BitwiseOr
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    IntegerColumn a = 3;
                    IntegerColumn b = 5;
                    DataFrame result = df.Select(
                        (a | b).As("a xor b"),
                        a.BitwiseOR(b).As("a xor b")
                    );

                    #endregion

                    return result.ShowMdString(showPlan: false);
                })
                .SaveResults();
    }
}
