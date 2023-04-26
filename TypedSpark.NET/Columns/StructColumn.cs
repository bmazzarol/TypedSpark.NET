using System;
using System.Linq;
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

    /// <summary>
    /// Migrates a struct column to a new schema
    /// </summary>
    /// <typeparam name="TB">new schema</typeparam>
    /// <returns>column</returns>
    public StructColumn<TB> Migrate<TB>()
        where TB : TypedSchema, new()
    {
        var newCol = new StructColumn<TB>();
        var existingColNames = ColumnType.Fields.ConvertAll(x => x.Name);
        var newColNames = newCol.ColumnType.Fields.ConvertAll(x => x.Name);
        var toAdd = newColNames.Except(existingColNames, StringComparer.Ordinal).ToList();
        var toRemove = existingColNames.Except(newColNames, StringComparer.Ordinal).ToArray();
        newCol.Column = Column;
        newCol.Column =
            toAdd.Count > 0
                ? toAdd.Aggregate(newCol.Column, (column, s) => column.WithField(s, F.Col(s)))
                : newCol.Column;
        newCol.Column = toRemove.Length > 0 ? newCol.Column.DropFields(toRemove) : newCol.Column;
        return newCol;
    }
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

    /// <summary>
    /// Builds a new struct type from existing columns that match the schema
    /// </summary>
    /// <typeparam name="T">schema type</typeparam>
    /// <returns>column</returns>
    public static StructColumn<T> FromColumns<T>()
        where T : TypedSchema, new()
    {
        var col = new StructColumn<T>();
        col.Column = F.Struct(col.ColumnType.Fields.ConvertAll(x => F.Col(x.Name)).ToArray());
        return col;
    }
}
