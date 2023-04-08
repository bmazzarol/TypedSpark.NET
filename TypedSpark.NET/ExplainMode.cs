namespace TypedSpark.NET;

/// <summary>
/// Supported explain modes
/// </summary>
public enum ExplainMode
{
    /// <summary>
    /// `simple` Print only a physical plan.
    /// </summary>
    Simple,

    /// <summary>
    /// `extended`: Print both logical and physical plans.
    /// </summary>
    Extended,

    /// <summary>
    /// `codegen`: Print a physical plan and generated codes if they are available.
    /// </summary>
    CodeGen,

    /// <summary>
    /// `cost`: Print a logical plan and statistics if they are available.
    /// </summary>
    Cost,

    /// <summary>
    /// `formatted`: Split explain output into two sections: a physical plan outline and
    /// node details.
    /// </summary>
    Formatted
}
