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

    /// <summary>
    /// Locates the position of the first occurrence of the value in the given array as long.
    /// Returns null if either of the arguments are null.
    /// </summary>
    /// <remarks>
    /// The position is not zero based, but 1 based index.
    /// Returns 0 if value could not be found in array.
    /// </remarks>
    /// <param name="value">Value to locate</param>
    /// <returns>Column object</returns>
    public IntegerColumn Position(T value) =>
        new() { Column = F.ArrayPosition(Column, (Column)value) };

    /// <summary>
    /// Remove all elements that equal to element from the given array.
    /// </summary>
    /// <param name="element">Element to remove</param>
    /// <returns>Column object</returns>
    public ArrayColumn<T> Remove(T element) =>
        new() { Column = F.ArrayRemove(Column, (Column)element) };

    /// <summary>
    /// Removes duplicate values from the array.
    /// </summary>
    /// <returns>Column object</returns>
    public ArrayColumn<T> Distinct() => new() { Column = F.ArrayDistinct(Column) };

    /// <summary>
    /// Returns an array of the elements in the intersection of the given two arrays,
    /// without duplicates.
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>Column object</returns>
    public ArrayColumn<T> Intersect(ArrayColumn<T> other) =>
        new() { Column = F.ArrayIntersect(Column, other) };

    /// <summary>
    /// Returns an array of the elements in the intersection of the given two arrays,
    /// without duplicates.
    /// </summary>
    /// <param name="lhs">Left side column to apply</param>
    /// <param name="rhs">Right side column to apply</param>
    /// <returns>Column object</returns>
    public static ArrayColumn<T> operator |(ArrayColumn<T> lhs, ArrayColumn<T> rhs) =>
        lhs.Intersect(rhs);

    /// <summary>
    /// Returns an array of the elements in the union of the given two arrays,
    /// without duplicates.
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>Column object</returns>
    public ArrayColumn<T> Union(ArrayColumn<T> other) =>
        new() { Column = F.ArrayUnion(Column, other) };

    /// <summary>
    /// Returns an array of the elements in the union of the given two arrays,
    /// without duplicates.
    /// </summary>
    /// <param name="lhs">Left side column to apply</param>
    /// <param name="rhs">Right side column to apply</param>
    /// <returns>Column object</returns>
    public static ArrayColumn<T> operator &(ArrayColumn<T> lhs, ArrayColumn<T> rhs) =>
        lhs.Union(rhs);

    /// <summary>
    /// Returns an array of the elements in the `col1` but not in the `col2`,
    /// without duplicates. The order of elements in the result is nondeterministic.
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>Column object</returns>
    public ArrayColumn<T> Except(ArrayColumn<T> other) =>
        new() { Column = F.ArrayExcept(Column, other) };

    /// <summary>
    /// Creates a new row for each element in the given array
    /// </summary>
    /// <returns>Column object</returns>
    public T Explode() => new() { Column = F.Explode(Column) };

    /// <summary>
    /// Creates a new row for each element in the given array or map column.
    /// Unlike Explode(), if the array/map is null or empty then null is produced.
    /// </summary>
    /// <returns>Column object</returns>
    public T ExplodeOuter() => new() { Column = F.ExplodeOuter(Column) };

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

    /// <summary>
    /// Concatenates the elements of `column` using the `delimiter`.
    /// </summary>
    /// <param name="column">column</param>
    /// <param name="delimiter">Delimiter for join</param>
    /// <param name="nullReplacement">String to replace null value</param>
    /// <returns>Column object</returns>
    public static StringColumn Join(
        this ArrayColumn<StringColumn> column,
        string delimiter,
        string? nullReplacement = default
    ) =>
        new()
        {
            Column =
                nullReplacement != null
                    ? F.ArrayJoin(column, delimiter, nullReplacement)
                    : F.ArrayJoin(column, delimiter)
        };

    /// <summary>
    /// Sorts the input array in ascending order.
    /// Null elements will be placed at the end of the returned array.
    /// </summary>
    /// <param name="column">Column to apply</param>
    /// <returns>Column object</returns>
    public static ArrayColumn<T> Sort<T>(this ArrayColumn<T> column)
        where T : TypedColumn, TypedOrdColumn, new() => new() { Column = F.ArraySort(column) };
}
