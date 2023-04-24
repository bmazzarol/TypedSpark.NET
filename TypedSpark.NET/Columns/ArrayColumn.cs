using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

public sealed class ArrayColumn<T> : TypedColumn<ArrayColumn<T>, ArrayType>
    where T : TypedColumn, new()
{
    internal ArrayColumn(Column column) : base(new ArrayType(new T().ColumnType), column) { }

    public ArrayColumn() : this(F.Col(string.Empty)) { }

    /// <summary>
    /// Creates a new row for each element in the given array
    /// </summary>
    /// <returns>Column object</returns>
    public T Explode() => new() { Column = F.Explode(Column) };

    /// <summary>
    /// Gets the value at the index
    /// </summary>
    /// <param name="index">index</param>
    public T this[int index] => new() { Column = Column.GetItem(index) };

    /// <summary>
    /// Returns null if the array is null, true if the array contains `value`,
    /// and false otherwise.
    /// </summary>
    /// <param name="value">Value to check for existence</param>
    /// <returns>Column object</returns>
    public BooleanColumn Contains(T value) =>
        new() { Column = F.ArrayContains(Column, (Column)value) };

    /// <summary>
    /// Returns true if the provided array column has at least one non-null element in common.
    /// If not and both arrays are non-empty and any of them contains a null,
    /// it returns null. It returns false otherwise.
    /// </summary>
    /// <param name="col">other array</param>
    /// <returns>Column object</returns>
    public BooleanColumn Overlaps(ArrayColumn<T> col) =>
        new() { Column = F.ArraysOverlap(Column, (Column)col) };

    /// <summary>
    /// Returns an array containing all the elements in `column` from index `start`
    /// (or starting from the end if `start` is negative) with the specified `length`.
    /// </summary>
    /// <param name="start">Start position in the array</param>
    /// <param name="length">Length for slicing</param>
    /// <returns>Column object</returns>
    public ArrayColumn<T> Slice(int start, int length) =>
        new() { Column = F.Slice(Column, start, length) };

    /// <summary>
    /// Returns an array containing all the elements in `column` from index `start`
    /// (or starting from the end if `start` is negative) with the specified `length`.
    /// </summary>
    /// <param name="start">Start position in the array</param>
    /// <param name="length">Length for slicing</param>
    /// <returns>Column object</returns>
    public ArrayColumn<T> Slice(IntegerColumn start, IntegerColumn length) =>
        new() { Column = F.Slice(Column, start, length) };
}

public static class ArrayColumn
{
    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static ArrayColumn<T> New<T>(string name, Column? column = default)
        where T : TypedColumn, new() => new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static ArrayColumn<T> New<T>(Column column) where T : TypedColumn, new() => new(column);

    /// <summary>
    /// Creates a new array column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="columns">existing columns of the correct type</param>
    /// <returns>column</returns>
    public static ArrayColumn<T> New<T>(string name, params T[] columns)
        where T : TypedColumn, new() =>
        new(F.Array(columns.Select(x => (Column)x).ToArray()).As(name));
}
