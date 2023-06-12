using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Array column
/// </summary>
public sealed class ArrayColumn<T> : TypedColumn<ArrayColumn<T>, ArrayType>
    where T : TypedColumn, new()
{
    internal ArrayColumn(Column column)
        : base(new ArrayType(new T().ColumnType), column) { }

    /// <summary>
    /// Constructs an empty array column
    /// </summary>
    public ArrayColumn()
        : this(F.Col(string.Empty)) { }

    private static ArrayColumn<T> New(Column column) => ArrayColumn.New<T>(column);

    private static T NewValue(Column column) => new() { Column = column };

    /// <summary>
    /// Gets the value at the index
    /// </summary>
    /// <param name="index">index</param>
    /// <returns>T column</returns>
    public T this[int index] => NewValue(Column.GetItem(index));

    /// <summary>
    /// Returns null if the array is null, true if the array contains `value`,
    /// and false otherwise
    /// </summary>
    /// <param name="value">Value to check for existence</param>
    /// <returns>boolean column</returns>
    public BooleanColumn Contains(T value) =>
        BooleanColumn.New(F.ArrayContains(Column, (Column)value));

    /// <summary>
    /// Returns true if the provided array column has at least one non-null element in common.
    /// If not and both arrays are non-empty and any of them contains a null,
    /// it returns null. It returns false otherwise.
    /// </summary>
    /// <param name="col">other array</param>
    /// <returns>boolean column</returns>
    [Since("2.4.0")]
    public BooleanColumn Overlaps(ArrayColumn<T> col) =>
        BooleanColumn.New(F.ArraysOverlap(Column, (Column)col));

    /// <summary>
    /// Returns an array containing all the elements in `column` from index `start`
    /// (or starting from the end if `start` is negative) with the specified `length`.
    /// </summary>
    /// <param name="start">Start position in the array</param>
    /// <param name="length">Length for slicing</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Slice(int start, int length) => New(F.Slice(Column, start, length));

    /// <summary>
    /// Returns an array containing all the elements in `column` from index `start`
    /// (or starting from the end if `start` is negative) with the specified `length`.
    /// </summary>
    /// <param name="start">Start position in the array</param>
    /// <param name="length">Length for slicing</param>
    /// <returns>array column</returns>
    [Since("3.1.0")]
    public ArrayColumn<T> Slice(IntegerColumn start, IntegerColumn length) =>
        New(F.Slice(Column, start, length));

    /// <summary>
    /// Locates the position of the first occurrence of the value in the given array as long.
    /// Returns null if either of the arguments are null.
    /// </summary>
    /// <remarks>
    /// The position is not zero based, but 1 based index.
    /// Returns 0 if value could not be found in array.
    /// </remarks>
    /// <param name="value">Value to locate</param>
    /// <returns>integer column</returns>
    [Since("2.4.0")]
    public IntegerColumn Position(T value) =>
        IntegerColumn.New(F.ArrayPosition(Column, (Column)value));

    /// <summary>
    /// Remove all elements that equal to element from the given array
    /// </summary>
    /// <param name="element">Element to remove</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Remove(T element) => New(F.ArrayRemove(Column, (Column)element));

    /// <summary>
    /// Removes duplicate values from the array
    /// </summary>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Distinct() => New(F.ArrayDistinct(Column));

    /// <summary>
    /// Returns an array of the elements in the intersection of the given two arrays,
    /// without duplicates
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Intersect(ArrayColumn<T> other) => New(F.ArrayIntersect(Column, other));

    /// <summary>
    /// Returns an array of the elements in the intersection of the given two arrays,
    /// without duplicates
    /// </summary>
    /// <param name="lhs">Left side column to apply</param>
    /// <param name="rhs">Right side column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> operator |(ArrayColumn<T> lhs, ArrayColumn<T> rhs) =>
        lhs.Intersect(rhs);

    /// <summary>
    /// Returns an array of the elements in the union of the given two arrays,
    /// without duplicates
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Union(ArrayColumn<T> other) => New(F.ArrayUnion(Column, other));

    /// <summary>
    /// Returns an array of the elements in the union of the given two arrays,
    /// without duplicates
    /// </summary>
    /// <param name="lhs">Left side column to apply</param>
    /// <param name="rhs">Right side column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> operator &(ArrayColumn<T> lhs, ArrayColumn<T> rhs) =>
        lhs.Union(rhs);

    /// <summary>
    /// Returns an array of the elements in the `col1` but not in the `col2`,
    /// without duplicates. The order of elements in the result is nondeterministic.
    /// </summary>
    /// <param name="other">Right side column to apply</param>
    /// <returns>Column object</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Except(ArrayColumn<T> other) => New(F.ArrayExcept(Column, other));

    /// <summary>
    /// Creates a new row for each element in the given array
    /// </summary>
    /// <returns>T column</returns>
    public T Explode() => NewValue(F.Explode(Column));

    /// <summary>
    /// Creates a new row for each element in the given array or map column.
    /// Unlike Explode(), if the array/map is null or empty then null is produced.
    /// </summary>
    /// <returns>T column</returns>
    public T ExplodeOuter() => NewValue(F.ExplodeOuter(Column));

    /// <summary>
    /// Creates a new row for each element with position in the given array column
    /// </summary>
    /// <returns>pos and col columns</returns>
    public ExplodedColumn PosExplode(out IntegerColumn pos, out T col)
    {
        pos = IntegerColumn.New("pos");
        col = NewValue(F.Col("col"));
        return new ExplodedColumn(F.PosExplode(Column));
    }

    /// <summary>
    /// Creates a new row for each element with position in the given array column.
    /// Unlike Posexplode(), if the array is null or empty then the row(null, null) is produced.
    /// </summary>
    /// <returns>pos and col columns</returns>
    public ExplodedColumn PosExplodeOuter(out IntegerColumn pos, out T col)
    {
        pos = IntegerColumn.New("pos");
        col = NewValue(F.Col("col"));
        return new ExplodedColumn(F.PosExplodeOuter(Column));
    }

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
    /// Returns a random permutation of the given array
    /// </summary>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Shuffle() => New(F.Shuffle(Column));

    /// <summary>
    /// Reverses the array column and returns it as a new array column
    /// </summary>
    /// <returns>array column</returns>
    public ArrayColumn<T> Reverse() => New(F.Reverse(Column));

    /// <summary>
    /// Filters the input array using the given predicate
    /// </summary>
    /// <param name="pred">predicate</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public ArrayColumn<T> Filter(Func<T, BooleanColumn> pred) =>
        New(F.Expr($"filter({Column}, x -> {pred(new T { Column = F.Col("x") })})"));

    /// <summary>
    /// Filters the input array using the given predicate
    /// </summary>
    /// <param name="pred">predicate</param>
    /// <returns>array column</returns>
    [Since("3.0.0")]
    public ArrayColumn<T> Filter(Func<T, IntegerColumn, BooleanColumn> pred) =>
        New(
            F.Expr(
                $"filter({Column}, (x, i) -> {pred(new T { Column = F.Col("x") }, IntegerColumn.New("i"))})"
            )
        );

    private TC InternalAggregate<TB, TC>(
        TB seed,
        Func<TB, T, TB> merge,
        Func<TB, TC>? project = default
    )
        where TB : TypedColumn, new()
        where TC : TypedColumn, new() =>
        new()
        {
            Column = F.Expr(
                $"aggregate({Column},{seed.Column},"
                    + $"(acc, x) -> {merge(new TB { Column = F.Col("acc") }, new T { Column = F.Col("x") })}"
                    + (
                        project != null
                            ? $",x -> {project(new TB { Column = F.Col("x") })}"
                            : string.Empty
                    )
                    + ")"
            )
        };

    /// <summary>
    /// Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state
    /// </summary>
    /// <param name="seed">seed</param>
    /// <param name="merge">merge function</param>
    /// <returns>aggregated state</returns>
    [Since("2.4.0")]
    public TB Aggregate<TB>(TB seed, Func<TB, T, TB> merge)
        where TB : TypedColumn, new() => InternalAggregate<TB, TB>(seed, merge);

    /// <summary>
    /// Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state
    /// </summary>
    /// <param name="seed">seed</param>
    /// <param name="merge">merge function</param>
    /// <param name="project">projection function</param>
    /// <returns>aggregated state</returns>
    [Since("2.4.0")]
    public TC Aggregate<TB, TC>(TB seed, Func<TB, T, TB> merge, Func<TB, TC> project)
        where TB : TypedColumn, new()
        where TC : TypedColumn, new() => InternalAggregate(seed, merge, project);
}

/// <summary>
/// Array columns static companion object
/// </summary>
public static class ArrayColumn
{
    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>array column</returns>
    public static ArrayColumn<T> New<T>(string name, Column? column = default)
        where T : TypedColumn, new() => new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>array column</returns>
    public static ArrayColumn<T> New<T>(Column column)
        where T : TypedColumn, new() => new(column);

    /// <summary>
    /// Creates a new array column
    /// </summary>
    /// <param name="columns">existing columns of the correct type</param>
    /// <returns>array column</returns>
    public static ArrayColumn<T> New<T>(params T[] columns)
        where T : TypedColumn, new() => New(columns.AsEnumerable());

    /// <summary>
    /// Creates a new array column
    /// </summary>
    /// <param name="columns">existing columns of the correct type</param>
    /// <returns>array column</returns>
    public static ArrayColumn<T> New<T>(IEnumerable<T> columns)
        where T : TypedColumn, new() => New<T>(F.Array(columns.Select(x => (Column)x).ToArray()));

    /// <summary>
    /// Concatenates the elements of `column` using the `delimiter`
    /// </summary>
    /// <param name="column">column</param>
    /// <param name="delimiter">Delimiter for join</param>
    /// <param name="nullReplacement">String to replace null value</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static StringColumn Join(
        this ArrayColumn<StringColumn> column,
        string delimiter,
        string? nullReplacement = default
    ) =>
        StringColumn.New(
            nullReplacement != null
                ? F.ArrayJoin(column, delimiter, nullReplacement)
                : F.ArrayJoin(column, delimiter)
        );

    /// <summary>
    /// Sorts the input array in ascending order.
    /// Null elements will be placed at the end of the returned array.
    /// </summary>
    /// <param name="column">Column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Sort<T>(this ArrayColumn<T> column)
        where T : TypedColumn, TypedOrdColumn, new() => New<T>(F.ArraySort(column));

    /// <summary>
    /// Returns the minimum value in the array
    /// </summary>
    /// <param name="column">Column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Min<T>(this ArrayColumn<T> column)
        where T : TypedColumn, TypedOrdColumn, new() => New<T>(F.ArrayMin(column));

    /// <summary>
    /// Returns the maximum value in the array
    /// </summary>
    /// <param name="column">Column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Max<T>(this ArrayColumn<T> column)
        where T : TypedColumn, TypedOrdColumn, new() => New<T>(F.ArrayMax(column));

    /// <summary>
    /// Creates a single array from an array of arrays. If a structure of nested arrays
    /// is deeper than two levels, only one level of nesting is removed.
    /// </summary>
    /// <param name="column">Column to apply</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Flatten<T>(this ArrayColumn<ArrayColumn<T>> column)
        where T : TypedColumn, new() => New<T>(F.Flatten(column));

    /// <summary>
    /// Generate a sequence of integers from `start` to `stop`, incrementing by `step`
    /// </summary>
    /// <param name="start">Start expression</param>
    /// <param name="stop">Stop expression</param>
    /// <param name="step">Step to increment</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<IntegerColumn> Sequence(
        IntegerColumn start,
        IntegerColumn stop,
        IntegerColumn? step = default
    ) => New<IntegerColumn>(F.Sequence(start, stop, step ?? 1));

    /// <summary>
    /// Generate a sequence of integers from `start` to `stop`, incrementing by `step`
    /// </summary>
    /// <param name="start">Start expression</param>
    /// <param name="stop">Stop expression</param>
    /// <param name="step">Step to increment</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<IntegerColumn> Range(
        IntegerColumn start,
        IntegerColumn stop,
        IntegerColumn? step = default
    ) => Sequence(start, stop, step);

    /// <summary>
    /// Creates an array containing the `left` argument repeated the number of times given by
    /// the `right` argument
    /// </summary>
    /// <param name="left">Left column expression</param>
    /// <param name="right">Right column expression</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Repeat<T>(this T left, IntegerColumn right)
        where T : TypedColumn, new() => New<T>(F.ArrayRepeat(left, right));

    [Pure]
    private static string ArrayTupleName(this string currentName, int index) =>
        currentName.StartsWith("array(", StringComparison.Ordinal)
        && currentName.EndsWith(")", StringComparison.Ordinal)
            ? index.ToString(CultureInfo.InvariantCulture)
            : currentName;

    /// <summary>
    /// Returns a merged array of structs in which the N-th struct contains all
    /// N-th values of input arrays
    /// </summary>
    /// <param name="first">first array</param>
    /// <param name="second">second array</param>
    /// <returns>map/struct column</returns>
    [Since("2.4.0")]
    public static Tuple2Column<TA, TB> Zip<TA, TB>(
        this ArrayColumn<TA> first,
        ArrayColumn<TB> second
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
    {
        var firstValue = new TA { Column = F.Col(first.ToString().ArrayTupleName(0)) };
        var secondValue = new TB { Column = F.Col(second.ToString().ArrayTupleName(1)) };
        return TupleColumn.New(firstValue, secondValue, F.ArraysZip(first, second));
    }

    /// <summary>
    /// Returns a merged array of structs in which the N-th struct contains all
    /// N-th values of input arrays
    /// </summary>
    /// <param name="first">first array</param>
    /// <param name="second">second array</param>
    /// <param name="third">third array</param>
    /// <returns>map/struct column</returns>
    [Since("2.4.0")]
    public static Tuple3Column<TA, TB, TC> Zip<TA, TB, TC>(
        this ArrayColumn<TA> first,
        ArrayColumn<TB> second,
        ArrayColumn<TC> third
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
        where TC : TypedColumn, new()
    {
        var firstValue = new TA { Column = F.Col(first.ToString().ArrayTupleName(0)) };
        var secondValue = new TB { Column = F.Col(second.ToString().ArrayTupleName(1)) };
        var thirdValue = new TC { Column = F.Col(third.ToString().ArrayTupleName(2)) };
        return TupleColumn.New(
            firstValue,
            secondValue,
            thirdValue,
            F.ArraysZip(first, second, third)
        );
    }

    /// <summary>
    /// Returns a merged array of structs in which the N-th struct contains all
    /// N-th values of input arrays
    /// </summary>
    /// <param name="first">first array</param>
    /// <param name="second">second array</param>
    /// <param name="third">third array</param>
    /// <param name="fourth">fourth array</param>
    /// <returns>map/struct column</returns>
    [Since("2.4.0")]
    public static Tuple4Column<TA, TB, TC, TD> Zip<TA, TB, TC, TD>(
        this ArrayColumn<TA> first,
        ArrayColumn<TB> second,
        ArrayColumn<TC> third,
        ArrayColumn<TD> fourth
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
        where TC : TypedColumn, new()
        where TD : TypedColumn, new()
    {
        var firstValue = new TA { Column = F.Col(first.ToString().ArrayTupleName(0)) };
        var secondValue = new TB { Column = F.Col(second.ToString().ArrayTupleName(1)) };
        var thirdValue = new TC { Column = F.Col(third.ToString().ArrayTupleName(2)) };
        var fourthValue = new TD { Column = F.Col(fourth.ToString().ArrayTupleName(3)) };
        return TupleColumn.New(
            firstValue,
            secondValue,
            thirdValue,
            fourthValue,
            F.ArraysZip(first, second, third, fourth)
        );
    }

    /// <summary>
    /// Returns a merged array of structs in which the N-th struct contains all
    /// N-th values of input arrays
    /// </summary>
    /// <param name="first">first array</param>
    /// <param name="second">second array</param>
    /// <param name="third">third array</param>
    /// <param name="fourth">fourth array</param>
    /// <param name="fifth">fifth array</param>
    /// <returns>map/struct column</returns>
    [Since("2.4.0")]
    public static Tuple5Column<TA, TB, TC, TD, TE> Zip<TA, TB, TC, TD, TE>(
        this ArrayColumn<TA> first,
        ArrayColumn<TB> second,
        ArrayColumn<TC> third,
        ArrayColumn<TD> fourth,
        ArrayColumn<TE> fifth
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
        where TC : TypedColumn, new()
        where TD : TypedColumn, new()
        where TE : TypedColumn, new()
    {
        var firstValue = new TA { Column = F.Col(first.ToString().ArrayTupleName(0)) };
        var secondValue = new TB { Column = F.Col(second.ToString().ArrayTupleName(1)) };
        var thirdValue = new TC { Column = F.Col(third.ToString().ArrayTupleName(2)) };
        var fourthValue = new TD { Column = F.Col(fourth.ToString().ArrayTupleName(3)) };
        var fifthValue = new TE { Column = F.Col(fifth.ToString().ArrayTupleName(4)) };
        return TupleColumn.New(
            firstValue,
            secondValue,
            thirdValue,
            fourthValue,
            fifthValue,
            F.ArraysZip(first, second, third, fourth, fifth)
        );
    }
}
