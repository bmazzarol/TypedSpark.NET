﻿using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Extensions;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

public class DecimalColumn : TypedNumericColumn<DecimalColumn, DecimalType, decimal>
{
    private DecimalColumn(string columnName, Column column)
        : base(columnName, new DecimalType(), column) { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static DecimalColumn New(string name, Column? column = default) =>
        new(name, column ?? Col(name));

    protected override DecimalColumn New(Column column, string? name = default) =>
        New(name ?? ColumnName, column);

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a double.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(ColumnName, Column.Cast("string"));

    public static implicit operator StringColumn(DecimalColumn column) => column.CastToString();

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public DecimalColumn IsIn(decimal first, params decimal[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
