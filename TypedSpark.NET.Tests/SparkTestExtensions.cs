using System;
using System.Linq;
using System.Text.RegularExpressions;
using BunsenBurner;
using Microsoft.Spark.Sql;
using VerifyXunit;
using static SparkTest.NET.SparkSessionFactory;
using Scenario = BunsenBurner.Scenario<BunsenBurner.Syntax.Aaa>;

namespace TypedSpark.NET.Test
{
    public static class SparkTestExtensions
    {
        internal static Scenario.Arranged<T> ArrangeUsingSpark<T>(Func<SparkSession, T> arrange) =>
            UseSession(arrange).ArrangeData();

        /// <summary>
        /// Replaces the non-deterministic parts of an explain plan result
        /// </summary>
        /// <param name="result">result of calling explain</param>
        /// <param name="removeIndexes">flag to indicate that indexes should be removed, not re-indexed</param>
        /// <returns>re-indexed explain plan</returns>
        private static string ReIndexExplainPlan(this string result, bool removeIndexes)
        {
            // strip out all the # index values that increment over the life of a spark session
            var indexes = result
                .Split("#")
                .Select(part =>
                {
                    var newIndex = new string(part.TakeWhile(char.IsDigit).ToArray());
                    return newIndex.Length > 0 ? $"#{newIndex}" : null;
                })
                .OfType<string>()
                .Distinct()
                .OrderBy(_ => _)
                .Select((index, i) => (Current: index, Replacement: $"#{i + 1}"))
                .OrderByDescending(_ => _);

            // replace them with stable indexes that are scope to the current plan
            return indexes.Aggregate(
                result,
                (s, pair) =>
                {
                    var target = $"({pair.Current})([^0-9]|$)";
                    var replace = $"{(!removeIndexes ? pair.Replacement : string.Empty)}$2";
                    return Regex.Replace(s, target, replace);
                }
            );
        }

        internal static Scenario.Asserted<
            TData,
            TypedDataFrame<TSchema>
        > AndExplainPlanHasNotChanged<TData, TSchema>(
            this Scenario.Asserted<TData, TypedDataFrame<TSchema>> scenario,
            ExplainMode mode = ExplainMode.Extended,
            bool removeIndexes = true
        ) =>
            scenario.And(
                s =>
                    Verifier
                        .Verify(
                            $"{s.Explain(mode).ReIndexExplainPlan(removeIndexes)}\n== DataFrame Schema ==\n{s.DataFrame.Schema().SimpleString}"
                        )
                        .UseDirectory("__snapshots__")
            );
    }
}
