using Docfx.ResultSnippets;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using TypedSpark.NET.Columns;
using Xunit;
using static SparkTest.NET.SparkSessionFactory;

namespace TypedSpark.NET.Tests.Examples
{
    public static class NotExamples
    {
        [Fact]
        public static void Case1() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example1

                    BooleanColumn col = true;
                    DataFrame result = df.Select(!col, col.Not());

                    #endregion

                    return result.ShowMdString();
                })
                .SaveResults();

        [Fact]
        public static void Case2() =>
            UseSession(s =>
                {
                    var df = s.CreateEmptyFrame();

                    #region Example2

                    BooleanColumn col = false;
                    DataFrame result = df.Select(!col, col.Not());

                    #endregion

                    return result.ShowMdString();
                })
                .SaveResults();
    }
}
