using System;
using System.Linq.Expressions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Struct column
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class StructColumn<T> : TypedColumn<StructColumn<T>, StructType>
    where T : TypedSchema, new()
{
    internal StructColumn(Column column)
        : base(new T().Type, column) { }

    public StructColumn()
        : this(F.Col(string.Empty)) { }

    /// <summary>
    /// An expression that gets a field by name in a `StructType`.
    /// </summary>
    /// <param name="getterExp">expression</param>
    /// <typeparam name="TA">column type</typeparam>
    /// <returns>column</returns>
    public TA Get<TA>(Expression<Func<T, TA>> getterExp)
        where TA : TypedColumn, new() =>
        new() { Column = Column.GetField(getterExp.GetPropertyInfo().Name) };
}

public static class StructColumn
{
    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static StructColumn<T> New<T>(string name, Column? column = default)
        where T : TypedSchema, new() => new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static StructColumn<T> New<T>(Column column)
        where T : TypedSchema, new() => new(column);
}
