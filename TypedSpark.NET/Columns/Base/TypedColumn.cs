#pragma warning disable CS0660, CS0661

using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Base objects in the typed column hierarchy
/// </summary>
public abstract class TypedColumn
{
    /// <summary>
    /// Underlying column this definition defines
    /// </summary>
    protected internal Column Column { get; set; }

    /// <summary>
    /// Underlying spark type of the column
    /// </summary>
    public DataType ColumnType { get; }

    /// <summary>
    /// Constructor that enforces that a typed column has a spark data type and underlying untyped column
    /// </summary>
    /// <param name="columnType">spark data type</param>
    /// <param name="column">untyped column</param>
    protected TypedColumn(DataType columnType, Column column)
    {
        ColumnType = columnType;
        Column = column;
    }

    /// <summary>
    /// <para>
    /// Internal mechanism used to convert a typed column to a dotnet value
    /// of the correct native type.
    /// </para>
    /// <para>This can be used by spark functions that only operate on literal values.</para>
    /// </summary>
    /// <remarks>
    /// This is not a safe operation and requires context at the call site to work.
    /// Its expected that implementing typed columns implement this only when its possible,
    /// and throw an unsupported operation in all other cases.
    /// </remarks>
    /// <returns>underlying dotnet type the column represents, or null</returns>
    protected internal abstract object? CoerceToNative();

    /// <summary>
    /// Prints the expression to the console for debugging purposes.
    /// </summary>
    /// <param name="extended">To print extended version or not</param>
    public void Explain(bool extended) => Column.Explain(extended);

    /// <inheritdoc />
    public override string ToString() => Column.ToString();

    /// <summary>
    /// Explicit cast from type to untyped column
    /// </summary>
    /// <param name="typedColumn">column</param>
    /// <returns>untyped column</returns>
    public static implicit operator Column(TypedColumn typedColumn) => typedColumn.Column;

    /// <summary>
    /// Casts the column to an untyped column
    /// </summary>
    /// <returns>Column object</returns>
    public Column CastToObject() => Column;
}

/// <summary>
/// Provides a typed version of a spark column
/// </summary>
/// <typeparam name="TThis">current extending type</typeparam>
/// <typeparam name="TSparkType">supported spark type</typeparam>
public abstract class TypedColumn<TThis, TSparkType> : TypedColumn
    where TThis : TypedColumn<TThis, TSparkType>, new()
    where TSparkType : DataType
{
    /// <summary>
    /// Underlying spark type of the column
    /// </summary>
    public new TSparkType ColumnType { get; }

    private static TThis New(Column column) => new() { Column = column };

    /// <inheritdoc />
    protected TypedColumn(TSparkType columnType, Column column)
        : base(columnType, column) => ColumnType = columnType;

    /// <inheritdoc />
    [SuppressMessage(
        "Design",
        "MA0025:Implement the functionality instead of throwing NotImplementedException"
    )]
    protected internal override object? CoerceToNative() =>
        throw new System.NotImplementedException();

    /// <summary>Apply equality test that is safe for null values.</summary>
    /// <param name="obj">Object to apply equality test</param>
    /// <returns>New column after applying the equality test</returns>
    public BooleanColumn EqNullSafe(TThis obj) => BooleanColumn.New(Column.EqNullSafe((Column)obj));

    /// <summary>
    /// Evaluates a condition and returns one of multiple possible result expressions.
    /// If Otherwise(object) is not defined at the end, null is returned for
    /// unmatched conditions. This method can be chained with other 'when' invocations in case
    /// multiple matches are required.
    /// </summary>
    /// <param name="condition">The condition to check</param>
    /// <param name="value">The value to set if the condition is true</param>
    /// <returns>New column after applying the when method</returns>
    public TThis When(BooleanColumn condition, TThis value) =>
        New(Column.When((Column)condition, (Column)value));

    /// <summary>
    /// Evaluates a list of conditions and returns one of multiple possible result expressions.
    /// If otherwise is not defined at the end, null is returned for unmatched conditions.
    /// This is used when the When(Column, object) method is applied.
    /// </summary>
    /// <param name="value">The value to set</param>
    /// <returns>New column after applying otherwise method</returns>
    public TThis Otherwise(TThis value) => New(Column.Otherwise((Column)value));

    /// <summary>
    /// True if the current column is between the lower bound and upper bound, inclusive.
    /// </summary>
    /// <param name="lowerBound">The lower bound</param>
    /// <param name="upperBound">The upper bound</param>
    /// <returns>New column after applying the between method</returns>
    public BooleanColumn Between(TThis lowerBound, TThis upperBound) =>
        BooleanColumn.New(Column.Between((Column)lowerBound, (Column)upperBound));

    /// <summary>True if the current expression is null.</summary>
    /// <returns>
    /// New column with values true if the preceding column had a null
    /// value in the same index, and false otherwise.
    /// </returns>
    public BooleanColumn IsNull() => BooleanColumn.New(Column.IsNull());

    /// <summary>True if the current expression is NOT null.</summary>
    /// <returns>
    /// New column with values true if the preceding column had a non-null
    /// value in the same index, and false otherwise.
    /// </returns>
    public BooleanColumn IsNotNull() => BooleanColumn.New(Column.IsNotNull());

    /// <summary>Gives the column an alias. Same as `As()`.</summary>
    /// <param name="alias">The alias that is given</param>
    /// <returns>New column after applying an alias</returns>
    public TThis Alias(string alias) => New(Column.Alias(alias));

    /// <summary>Gives the column an alias.</summary>
    /// <param name="alias">The alias that is given</param>
    /// <returns>New column after applying the as alias operator</returns>
    public TThis As(string alias) => Alias(alias);

    /// <summary>Gives the column a name (alias).</summary>
    /// <param name="alias">Alias column name</param>
    /// <returns>Column object</returns>
    public TThis Name(string alias) => Alias(alias);

    /// <summary>
    /// Returns a sort expression based on ascending order of the column,
    /// and null values return before non-null values.
    /// </summary>
    /// <returns>New column after applying the descending order operator</returns>
    public TThis Desc() => New(Column.Desc());

    /// <summary>
    /// Returns a sort expression based on the descending order of the column,
    /// and null values appear before non-null values.
    /// </summary>
    /// <returns>Column object</returns>
    public TThis DescNullsFirst() => New(Column.DescNullsFirst());

    /// <summary>
    /// Returns a sort expression based on the descending order of the column,
    /// and null values appear after non-null values.
    /// </summary>
    /// <returns>Column object</returns>
    public TThis DescNullsLast() => New(Column.DescNullsLast());

    /// <summary>
    /// Returns a sort expression based on ascending order of the column.
    /// </summary>
    /// <returns>New column after applying the ascending order operator</returns>
    public TThis Asc() => New(Column.Asc());

    /// <summary>
    /// Returns a sort expression based on ascending order of the column,
    /// and null values return before non-null values.
    /// </summary>
    /// <returns></returns>
    public TThis AscNullsFirst() => New(Column.AscNullsFirst());

    /// <summary>
    /// Returns a sort expression based on ascending order of the column,
    /// and null values appear after non-null values.
    /// </summary>
    /// <returns></returns>
    public TThis AscNullsLast() => New(Column.AscNullsLast());

    /// <summary>Defines a windowing column.</summary>
    /// <param name="window">
    /// A window specification that defines the partitioning, ordering, and frame boundaries.
    /// </param>
    /// <returns>Column object</returns>
    public TThis Over(WindowSpec window) => New(Column.Over(window));

    /// <summary>
    /// Defines an empty analytic clause. In this case the analytic function is applied
    /// and presented for all rows in the result set.
    /// </summary>
    /// <returns>Column object</returns>
    public TThis Over() => New(Column.Over());
}

/// <summary>
/// Provides a typed version of a spark column
/// </summary>
/// <typeparam name="TThis">current extending type</typeparam>
/// <typeparam name="TSparkType">supported spark type</typeparam>
/// <typeparam name="TNativeType">native dotnet type</typeparam>
[SuppressMessage(
    "Blocker Code Smell",
    "S3875:\"operator==\" should not be overloaded on reference types",
    Justification = "Spark equaility is required, all operations on a typed column are just proxy operations to Spark"
)]
public abstract class TypedColumn<TThis, TSparkType, TNativeType> : TypedColumn<TThis, TSparkType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    /// <summary>
    /// Underlying spark type of the column
    /// </summary>
    public new TSparkType ColumnType { get; }

    /// <inheritdoc />
    protected TypedColumn(TSparkType columnType, Column column)
        : base(columnType, column) => ColumnType = columnType;

    /// <summary>
    /// Equals
    /// </summary>
    public static BooleanColumn operator ==(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TypedColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.EqualTo(rhs);

    /// <summary>
    /// Equals
    /// </summary>
    public static BooleanColumn operator ==(
        TNativeType lhs,
        TypedColumn<TThis, TSparkType, TNativeType> rhs
    ) => BooleanColumn.New(F.Lit(lhs).EqualTo(rhs.Column));

    /// <summary>
    /// Equals
    /// </summary>
    public static BooleanColumn operator ==(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.EqualTo(rhs);

    /// <summary>
    /// Equals
    /// </summary>
    public BooleanColumn EqualTo(TypedColumn<TThis, TSparkType, TNativeType> other) =>
        BooleanColumn.New(Column.EqualTo((Column)other));

    /// <summary>
    /// Equals
    /// </summary>
    public BooleanColumn EqualTo(TNativeType other) =>
        BooleanColumn.New(Column.EqualTo(F.Lit(other)));

    /// <summary>
    /// Not Equals
    /// </summary>
    public static BooleanColumn operator !=(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TypedColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.NotEqual(rhs);

    /// <summary>
    /// Not Equals
    /// </summary>
    public static BooleanColumn operator !=(
        TNativeType lhs,
        TypedColumn<TThis, TSparkType, TNativeType> rhs
    ) => BooleanColumn.New(F.Lit(lhs).NotEqual(rhs.Column));

    /// <summary>
    /// Not Equals
    /// </summary>
    public static BooleanColumn operator !=(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.NotEqual(rhs);

    /// <summary>
    /// Not Equals
    /// </summary>
    public BooleanColumn NotEqual(TypedColumn<TThis, TSparkType, TNativeType> other) =>
        BooleanColumn.New(Column.NotEqual((Column)other));

    /// <summary>
    /// Not Equals
    /// </summary>
    public BooleanColumn NotEqual(TNativeType other) =>
        BooleanColumn.New(Column.NotEqual(F.Lit(other)));
}
