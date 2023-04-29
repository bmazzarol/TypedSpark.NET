using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Map column
/// </summary>
public sealed class MapColumn<TKey, TValue> : TypedColumn<MapColumn<TKey, TValue>, MapType>
    where TKey : TypedColumn, TypedOrdColumn, new()
    where TValue : TypedColumn, new()
{
    internal MapColumn(Column column)
        : base(new MapType(new TKey().ColumnType, new TValue().ColumnType), column) { }

    public MapColumn()
        : this(F.Col(string.Empty)) { }

    /// <summary>
    /// Gets the value at the key
    /// </summary>
    /// <param name="key">key</param>
    public TValue this[TKey key] => new() { Column = Column.GetItem(key.CoerceToNative()) };

    /// <summary>
    /// Returns length of array
    /// </summary>
    /// <returns>Column object</returns>
    public IntegerColumn Size() => new() { Column = F.Size(Column) };

    /// <summary>
    /// Returns length of array
    /// </summary>
    /// <returns>Column object</returns>
    public IntegerColumn Length() => Size();

    /// <summary>
    /// Map keys
    /// </summary>
    /// <returns>Column object</returns>
    public ArrayColumn<TKey> Keys() => new() { Column = F.MapKeys(Column) };

    /// <summary>
    /// Map values
    /// </summary>
    /// <returns>Column object</returns>
    public ArrayColumn<TValue> Values() => new() { Column = F.MapValues(Column) };

    /// <summary>
    /// Returns an unordered array of all entries in the given map.
    /// </summary>
    /// <returns>Column object</returns>
    public StructColumn<EntrySchema<TKey, TValue>> Entries() =>
        new() { Column = F.MapEntries(Column) };

    /// <summary>
    /// Returns a map of the elements in the union of the given two maps.
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>Column object</returns>
    public MapColumn<TKey, TValue> Union(MapColumn<TKey, TValue> other) =>
        MapColumn.Concat(this, other);

    /// <summary>
    /// Returns a map of the elements in the union of the given two maps.
    /// </summary>
    /// <param name="lhs">Left side column to apply</param>
    /// <param name="rhs">Right side column to apply</param>
    /// <returns>Column object</returns>
    public static MapColumn<TKey, TValue> operator &(
        MapColumn<TKey, TValue> lhs,
        MapColumn<TKey, TValue> rhs
    ) => lhs.Union(rhs);

    /// <summary>
    /// Filters entries in a map using the function
    /// </summary>
    /// <param name="pred">predicate</param>
    /// <returns>map column</returns>
    [Since("3.0.0")]
    public MapColumn<TKey, TValue> Filter(Func<TKey, TValue, BooleanColumn> pred)
    {
        var res =
            $"map_filter({Column}, (k, v) -> {pred(new TKey { Column = F.Col("k") }, new TValue { Column = F.Col("v") })})";
        return new MapColumn<TKey, TValue>
        {
            Column = F.Expr(res)
        };
    }
}

public static class MapColumn
{
    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static MapColumn<TKey, TValue> New<TKey, TValue>(string name, Column? column = default)
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() => new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static MapColumn<TKey, TValue> New<TKey, TValue>(Column column)
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() => new(column);

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="values">map values</param>
    /// <returns>column</returns>
    public static MapColumn<TKey, TValue> New<TKey, TValue>(
        params KeyValuePair<TKey, TValue>[] values
    )
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() =>
        new()
        {
            Column = F.Map(
                values.SelectMany(x => new[] { (Column)x.Key, (Column)x.Value }).ToArray()
            )
        };

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="keys">keys</param>
    /// <param name="values">values</param>
    /// <returns>column</returns>
    public static MapColumn<TKey, TValue> New<TKey, TValue>(
        ArrayColumn<TKey> keys,
        ArrayColumn<TValue> values
    )
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() => new() { Column = F.MapFromArrays(keys, values) };

    /// <summary>
    /// Returns the union of all the given maps.
    /// </summary>
    /// <param name="maps">maps</param>
    /// <returns>column</returns>
    public static MapColumn<TKey, TValue> Concat<TKey, TValue>(
        params MapColumn<TKey, TValue>[] maps
    )
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() =>
        new() { Column = F.MapConcat(maps.Select(x => (Column)x).ToArray()) };
}
