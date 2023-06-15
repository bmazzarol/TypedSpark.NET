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

    /// <summary>
    /// Constructs an empty column
    /// </summary>
    public MapColumn()
        : this(F.Col(string.Empty)) { }

    private static MapColumn<TKey, TValue> New(Column column) =>
        MapColumn.New<TKey, TValue>(column);

    private static TKey NewKey(Column column) => new() { Column = column };

    private static TValue NewValue(Column column) => new() { Column = column };

    /// <summary>
    /// Gets the value at the key
    /// </summary>
    /// <param name="key">key</param>
    /// <returns>TValue column</returns>
    public TValue this[TKey key] => NewValue(Column.GetItem(key.CoerceToNative()));

    /// <summary>
    /// Returns length of array
    /// </summary>
    /// <returns>integer column</returns>
    public IntegerColumn Size() => IntegerColumn.New(F.Size(Column));

    /// <summary>
    /// Returns length of array
    /// </summary>
    /// <returns>integer column</returns>
    public IntegerColumn Length() => Size();

    /// <summary>
    /// Map keys
    /// </summary>
    /// <returns>array of TKey column</returns>
    public ArrayColumn<TKey> Keys() => ArrayColumn.New<TKey>(F.MapKeys(Column));

    /// <summary>
    /// Map values
    /// </summary>
    /// <returns>array of TValue column</returns>
    public ArrayColumn<TValue> Values() => ArrayColumn.New<TValue>(F.MapValues(Column));

    /// <summary>
    /// Returns an unordered array of all entries in the given map
    /// </summary>
    /// <returns>struct of key value</returns>
    [Since("3.0.0")]
    public Tuple2Column<TKey, TValue> Entries() =>
        TupleColumn.New(NewKey(F.Col("key")), NewValue(F.Col("value")), F.MapEntries(Column));

    /// <summary>
    /// Returns a map of the elements in the union of the given two maps
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>map column</returns>
    public MapColumn<TKey, TValue> Union(MapColumn<TKey, TValue> other) =>
        MapColumn.Concat(this, other);

    /// <summary>
    /// Returns a map of the elements in the union of the given two maps
    /// </summary>
    /// <param name="lhs">Left side column to apply</param>
    /// <param name="rhs">Right side column to apply</param>
    /// <returns>map column</returns>
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
    public MapColumn<TKey, TValue> Filter(Func<TKey, TValue, BooleanColumn> pred) =>
        New(
            F.Expr(
                $"map_filter({Column}, (k, v) -> {pred(new TKey { Column = F.Col("k") }, new TValue { Column = F.Col("v") })})"
            )
        );

    /// <summary>
    /// Creates a new row for each element in the given map column
    /// </summary>
    /// <returns>key value columns</returns>
    public VoidColumn Explode(out TKey key, out TValue value)
    {
        key = NewKey(F.Col("key"));
        value = NewValue(F.Col("value"));
        return new VoidColumn(F.Explode(Column));
    }

    /// <summary>
    /// Creates a new row for each element in the given map column.
    /// Unlike Explode(), if the map is null or empty then null is produced.
    /// </summary>
    /// <returns>key value columns</returns>
    public VoidColumn ExplodeOuter(out TKey key, out TValue value)
    {
        key = NewKey(F.Col("key"));
        value = NewValue(F.Col("value"));
        return new VoidColumn(F.ExplodeOuter(Column));
    }

    /// <summary>
    /// Creates a new row for each element with position in the given map column
    /// </summary>
    /// <returns>pos, key, value columns</returns>
    public VoidColumn PosExplode(out IntegerColumn pos, out TKey key, out TValue value)
    {
        pos = IntegerColumn.New("pos");
        key = NewKey(F.Col("key"));
        value = NewValue(F.Col("value"));
        return new VoidColumn(F.PosExplode(Column));
    }

    /// <summary>
    /// Creates a new row for each element with position in the given map column.
    /// Unlike Posexplode(), if the map is null or empty then the row(null, null) is produced.
    /// </summary>
    /// <returns>pos, key and value columns</returns>
    public VoidColumn PosExplodeOuter(out IntegerColumn pos, out TKey key, out TValue value)
    {
        pos = IntegerColumn.New("pos");
        key = NewKey(F.Col("key"));
        value = NewValue(F.Col("value"));
        return new VoidColumn(F.PosExplodeOuter(Column));
    }
}

/// <summary>
/// Static companion class for MapColumn
/// </summary>
public static class MapColumn
{
    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>map column</returns>
    public static MapColumn<TKey, TValue> New<TKey, TValue>(string name, Column? column = default)
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() => new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>map column</returns>
    public static MapColumn<TKey, TValue> New<TKey, TValue>(Column column)
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() => new(column);

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="values">map values</param>
    /// <returns>map column</returns>
    public static MapColumn<TKey, TValue> New<TKey, TValue>(
        params KeyValuePair<TKey, TValue>[] values
    )
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() =>
        New<TKey, TValue>(
            F.Map(values.SelectMany(x => new[] { (Column)x.Key, (Column)x.Value }).ToArray())
        );

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="keys">keys</param>
    /// <param name="values">values</param>
    /// <returns>map column</returns>
    [Since("2.4.0")]
    public static MapColumn<TKey, TValue> New<TKey, TValue>(
        ArrayColumn<TKey> keys,
        ArrayColumn<TValue> values
    )
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() => New<TKey, TValue>(F.MapFromArrays(keys, values));

    /// <summary>
    /// Returns the union of all the given maps
    /// </summary>
    /// <param name="maps">maps</param>
    /// <returns>map column</returns>
    [Since("2.4.0")]
    public static MapColumn<TKey, TValue> Concat<TKey, TValue>(
        params MapColumn<TKey, TValue>[] maps
    )
        where TKey : TypedColumn, TypedOrdColumn, new()
        where TValue : TypedColumn, new() =>
        New<TKey, TValue>(F.MapConcat(maps.Select(x => (Column)x).ToArray()));
}
