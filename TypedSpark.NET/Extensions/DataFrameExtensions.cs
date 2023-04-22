using System;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace TypedSpark.NET;

/// <summary>
/// Extensions on data frames
/// </summary>
public static class DataFrameExtensions
{
    /// <summary>
    /// Returns the plans (logical and physical) with a format specified by a given explain mode.
    /// </summary>
    /// <param name="dataFrame">data frame</param>
    /// <param name="mode">Specifies the expected output format of plans. </param>
    public static string Explain(
        this DataFrame dataFrame,
        ExplainMode mode = ExplainMode.Extended
    ) =>
        (string)
            ((JvmObjectReference)dataFrame.Reference.Invoke("queryExecution")).Invoke(
                "explainString",
                (JvmObjectReference)
                    dataFrame.Reference.Jvm.CallStaticJavaMethod(
                        "org.apache.spark.sql.execution.ExplainMode",
                        "fromString",
                        mode switch
                        {
                            ExplainMode.Simple => "simple",
                            ExplainMode.Extended => "extended",
                            ExplainMode.CodeGen => "codegen",
                            ExplainMode.Cost => "cost",
                            ExplainMode.Formatted => "formatted",
                            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
                        }
                    )
            );

    /// <summary>
    /// Returns the plans (logical and physical) with a format specified by a given explain mode.
    /// </summary>
    /// <param name="dataFrame">data frame</param>
    /// <param name="mode">Specifies the expected output format of plans. </param>
    public static string Explain<T>(
        this TypedDataFrame<T> dataFrame,
        ExplainMode mode = ExplainMode.Extended
    ) => dataFrame.DataFrame.Explain(mode);
}
