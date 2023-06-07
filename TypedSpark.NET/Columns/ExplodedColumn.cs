using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Exploded column, has no operations as the column is no longer singular and has been exploded out
/// </summary>
public class ExplodedColumn : TypedColumn
{
    internal ExplodedColumn(Column column)
        : base(new NullType(), column) { }

    /// <inheritdoc />
    [SuppressMessage(
        "Design",
        "MA0025:Implement the functionality instead of throwing NotImplementedException"
    )]
    protected internal override object CoerceToNative() =>
        throw new System.NotImplementedException();
}
