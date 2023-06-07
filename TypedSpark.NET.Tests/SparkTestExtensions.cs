using System;
using System.Runtime.CompilerServices;
using BunsenBurner;
using BunsenBurner.Verify.Xunit;
using Microsoft.Spark.Sql;
using SparkTest.NET.Extensions;
using static SparkTest.NET.SparkSessionFactory;
using Scenario = BunsenBurner.Scenario<BunsenBurner.Syntax.Aaa>;

namespace TypedSpark.NET.Tests
{
    public static class SparkTestExtensions
    {
        internal static Scenario.Arranged<T> ArrangeUsingSpark<T>(Func<SparkSession, T> arrange) =>
            UseSession(arrange).ArrangeData();

        internal static Scenario.Asserted<
            TypedDataFrame<TSchema>,
            string
        > SnapshotDataframe<TSchema>(
            Func<SparkSession, TypedDataFrame<TSchema>> dfFn,
            [CallerFilePath] string sourceFilePath = ""
        ) =>
            ArrangeUsingSpark(dfFn)
                .Act(df => $"{df.Explain().ReIndexExplainPlan(true)}\n{((DataFrame)df).Debug()}")
                .AssertResultIsUnchanged(
                    sourceFilePath: sourceFilePath,
                    matchConfiguration: x => x.ScrubLinesContaining("+- FileScan json")
                );

        internal static Scenario.Asserted<DataFrame, string> DebugDataframe(
            Func<SparkSession, DataFrame> dfFn,
            [CallerFilePath] string sourceFilePath = ""
        ) =>
            ArrangeUsingSpark(dfFn)
                .Act(df => df.Debug())
                .AssertResultIsUnchanged(sourceFilePath: sourceFilePath);
    }
}
