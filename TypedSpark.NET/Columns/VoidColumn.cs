using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Void column, has no operations
/// </summary>
public class VoidColumn : TypedColumn
{
    internal VoidColumn(Column column)
        : base(new NullType(), column) { }

    /// <inheritdoc />
    [SuppressMessage(
        "Design",
        "MA0025:Implement the functionality instead of throwing NotImplementedException"
    )]
    protected internal override object CoerceToNative() =>
        throw new System.NotImplementedException();
}
