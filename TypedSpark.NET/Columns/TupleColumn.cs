#pragma warning disable CS1591

using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Tuple 2
/// </summary>
/// <typeparam name="TA">column 1</typeparam>
/// <typeparam name="TB">column 2</typeparam>
public sealed class Tuple2Column<TA, TB> : StructColumn<Tuple2Column<TA, TB>.Schema>
    where TA : TypedColumn, new()
    where TB : TypedColumn, new()
{
    internal Tuple2Column(TA item1, TB item2, Column? column = default)
        : base(
            new Schema(item1, item2),
            column ?? F.Struct(F.Col(item1.ToString()), F.Col(item2.ToString()))
        ) { }

    public sealed class Schema : TypedSchema
    {
        public TA Item1 { get; }
        public TB Item2 { get; }

        internal Schema(TA item1, TB item2)
            : base(
                new StructType(
                    new[]
                    {
                        new StructField(item1.ToString(), item1.ColumnType),
                        new StructField(item2.ToString(), item2.ColumnType)
                    }
                ),
                new Column[] { item1, item2 }
            )
        {
            Item1 = item1;
            Item2 = item2;
        }

        [ExcludeFromCodeCoverage]
        public Schema()
            : this(
                new TA { Column = F.Col(nameof(Item1)) },
                new TB { Column = F.Col(nameof(Item2)) }
            ) { }
    }
}

/// <summary>
/// Tuple 3
/// </summary>
/// <typeparam name="TA">column 1</typeparam>
/// <typeparam name="TB">column 2</typeparam>
/// <typeparam name="TC">column 3</typeparam>
public sealed class Tuple3Column<TA, TB, TC> : StructColumn<Tuple3Column<TA, TB, TC>.Schema>
    where TA : TypedColumn, new()
    where TB : TypedColumn, new()
    where TC : TypedColumn, new()
{
    internal Tuple3Column(TA item1, TB item2, TC item3, Column? column = default)
        : base(
            new Schema(item1, item2, item3),
            column
                ?? F.Struct(
                    F.Col(item1.ToString()),
                    F.Col(item2.ToString()),
                    F.Col(item3.ToString())
                )
        ) { }

    public sealed class Schema : TypedSchema
    {
        public TA Item1 { get; }
        public TB Item2 { get; }
        public TC Item3 { get; }

        internal Schema(TA item1, TB item2, TC item3)
            : base(
                new StructType(
                    new[]
                    {
                        new StructField(item1.ToString(), item1.ColumnType),
                        new StructField(item2.ToString(), item2.ColumnType),
                        new StructField(item3.ToString(), item3.ColumnType),
                    }
                ),
                new Column[] { item1, item2, item3 }
            )
        {
            Item1 = item1;
            Item2 = item2;
            Item3 = item3;
        }

        [ExcludeFromCodeCoverage]
        public Schema()
            : this(
                new TA { Column = F.Col(nameof(Item1)) },
                new TB { Column = F.Col(nameof(Item2)) },
                new TC { Column = F.Col(nameof(Item3)) }
            ) { }
    }
}

/// <summary>
/// Tuple 4
/// </summary>
/// <typeparam name="TA">column 1</typeparam>
/// <typeparam name="TB">column 2</typeparam>
/// <typeparam name="TC">column 3</typeparam>
/// <typeparam name="TD">column 4</typeparam>
public sealed class Tuple4Column<TA, TB, TC, TD> : StructColumn<Tuple4Column<TA, TB, TC, TD>.Schema>
    where TA : TypedColumn, new()
    where TB : TypedColumn, new()
    where TC : TypedColumn, new()
    where TD : TypedColumn, new()
{
    internal Tuple4Column(TA item1, TB item2, TC item3, TD item4, Column? column = default)
        : base(
            new Schema(item1, item2, item3, item4),
            column
                ?? F.Struct(
                    F.Col(item1.ToString()),
                    F.Col(item2.ToString()),
                    F.Col(item3.ToString()),
                    F.Col(item4.ToString())
                )
        ) { }

    public sealed class Schema : TypedSchema
    {
        public TA Item1 { get; }
        public TB Item2 { get; }
        public TC Item3 { get; }
        public TD Item4 { get; }

        internal Schema(TA item1, TB item2, TC item3, TD item4)
            : base(
                new StructType(
                    new[]
                    {
                        new StructField(item1.ToString(), item1.ColumnType),
                        new StructField(item2.ToString(), item2.ColumnType),
                        new StructField(item3.ToString(), item3.ColumnType),
                        new StructField(item4.ToString(), item4.ColumnType),
                    }
                ),
                new Column[] { item1, item2, item3, item4 }
            )
        {
            Item1 = item1;
            Item2 = item2;
            Item3 = item3;
            Item4 = item4;
        }

        [ExcludeFromCodeCoverage]
        public Schema()
            : this(
                new TA { Column = F.Col(nameof(Item1)) },
                new TB { Column = F.Col(nameof(Item2)) },
                new TC { Column = F.Col(nameof(Item3)) },
                new TD { Column = F.Col(nameof(Item4)) }
            ) { }
    }
}

/// <summary>
/// Tuple 5
/// </summary>
/// <typeparam name="TA">column 1</typeparam>
/// <typeparam name="TB">column 2</typeparam>
/// <typeparam name="TC">column 3</typeparam>
/// <typeparam name="TD">column 4</typeparam>
/// <typeparam name="TE">column 5</typeparam>
public sealed class Tuple5Column<TA, TB, TC, TD, TE>
    : StructColumn<Tuple5Column<TA, TB, TC, TD, TE>.Schema>
    where TA : TypedColumn, new()
    where TB : TypedColumn, new()
    where TC : TypedColumn, new()
    where TD : TypedColumn, new()
    where TE : TypedColumn, new()
{
    internal Tuple5Column(
        TA item1,
        TB item2,
        TC item3,
        TD item4,
        TE item5,
        Column? column = default
    )
        : base(
            new Schema(item1, item2, item3, item4, item5),
            column
                ?? F.Struct(
                    F.Col(item1.ToString()),
                    F.Col(item2.ToString()),
                    F.Col(item3.ToString()),
                    F.Col(item4.ToString()),
                    F.Col(item5.ToString())
                )
        ) { }

    public sealed class Schema : TypedSchema
    {
        public TA Item1 { get; }
        public TB Item2 { get; }
        public TC Item3 { get; }
        public TD Item4 { get; }
        public TE Item5 { get; }

        internal Schema(TA item1, TB item2, TC item3, TD item4, TE item5)
            : base(
                new StructType(
                    new[]
                    {
                        new StructField(item1.ToString(), item1.ColumnType),
                        new StructField(item2.ToString(), item2.ColumnType),
                        new StructField(item3.ToString(), item3.ColumnType),
                        new StructField(item4.ToString(), item4.ColumnType),
                        new StructField(item5.ToString(), item5.ColumnType),
                    }
                ),
                new Column[] { item1, item2, item3, item4, item5 }
            )
        {
            Item1 = item1;
            Item2 = item2;
            Item3 = item3;
            Item4 = item4;
            Item5 = item5;
        }

        [ExcludeFromCodeCoverage]
        public Schema()
            : this(
                new TA { Column = F.Col(nameof(Item1)) },
                new TB { Column = F.Col(nameof(Item2)) },
                new TC { Column = F.Col(nameof(Item3)) },
                new TD { Column = F.Col(nameof(Item4)) },
                new TE { Column = F.Col(nameof(Item5)) }
            ) { }
    }
}

/// <summary>
/// Construction functions for tuples. Tuples are just structs with user provided column names
/// </summary>
public static class TupleColumn
{
    /// <summary>
    /// Creates a new tuple column
    /// </summary>
    /// <param name="item1">item 1</param>
    /// <param name="item2">item 2</param>
    /// <param name="column">optional column</param>
    /// <returns>tuple column</returns>
    public static Tuple2Column<TA, TB> New<TA, TB>(TA item1, TB item2, Column? column = default)
        where TA : TypedColumn, new()
        where TB : TypedColumn, new() => new(item1, item2, column);

    /// <summary>
    /// Creates a new tuple column
    /// </summary>
    /// <param name="item1">item 1</param>
    /// <param name="item2">item 2</param>
    /// <param name="item3">item 3</param>
    /// <param name="column">optional column</param>
    /// <returns>tuple column</returns>
    public static Tuple3Column<TA, TB, TC> New<TA, TB, TC>(
        TA item1,
        TB item2,
        TC item3,
        Column? column = default
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
        where TC : TypedColumn, new() => new(item1, item2, item3, column);

    /// <summary>
    /// Creates a new tuple column
    /// </summary>
    /// <param name="item1">item 1</param>
    /// <param name="item2">item 2</param>
    /// <param name="item3">item 3</param>
    /// <param name="item4">item 4</param>
    /// <param name="column">optional column</param>
    /// <returns>tuple column</returns>
    public static Tuple4Column<TA, TB, TC, TD> New<TA, TB, TC, TD>(
        TA item1,
        TB item2,
        TC item3,
        TD item4,
        Column? column = default
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
        where TC : TypedColumn, new()
        where TD : TypedColumn, new() => new(item1, item2, item3, item4, column);

    /// <summary>
    /// Creates a new tuple column
    /// </summary>
    /// <param name="item1">item 1</param>
    /// <param name="item2">item 2</param>
    /// <param name="item3">item 3</param>
    /// <param name="item4">item 4</param>
    /// <param name="item5">item 5</param>
    /// <param name="column">optional column</param>
    /// <returns>tuple column</returns>
    public static Tuple5Column<TA, TB, TC, TD, TE> New<TA, TB, TC, TD, TE>(
        TA item1,
        TB item2,
        TC item3,
        TD item4,
        TE item5,
        Column? column = default
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
        where TC : TypedColumn, new()
        where TD : TypedColumn, new()
        where TE : TypedColumn, new() => new(item1, item2, item3, item4, item5, column);
}
