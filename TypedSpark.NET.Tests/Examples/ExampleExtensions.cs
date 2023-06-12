using System;
using System.Runtime.CompilerServices;
using Docfx.ResultSnippets;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using Scenario = BunsenBurner.Scenario<BunsenBurner.Syntax.Aaa>;

namespace TypedSpark.NET.Tests.Examples
{
    public static class ExampleExtensions
    {
        internal static Scenario.Asserted<DataFrame, string> DebugDataframeAndSaveExample(
            Func<SparkSession, DataFrame> dfFn,
            [CallerFilePath] string sourceFilePath = "",
            [CallerMemberName] string memberName = ""
        ) =>
            SparkTestExtensions.DebugDataframe(session =>
            {
                var result = dfFn(session);

                result
                    .ShowMdString(showPlan: false)
                    .SaveResults(sourceFilePath: sourceFilePath, memberName: memberName);

                return result;
            });
    }
}
