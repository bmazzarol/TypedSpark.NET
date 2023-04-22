using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

public sealed class StringColumn : TypedOrdColumn<StringColumn, StringType, string>
{
    private StringColumn(Column column) : base(new StringType(), column) { }

    public StringColumn() : this(Col(string.Empty)) { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static StringColumn New(string name, Column? column = default) =>
        new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static StringColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator StringColumn(string lit) => New(Lit(lit));

    /// <summary>Apply sum of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the sum operation</returns>
    public static StringColumn operator +(StringColumn lhs, StringColumn rhs) => lhs.Plus(rhs);

    /// <summary>Sum of this expression and another expression.</summary>
    /// <param name="rhs">The expression to be summed with</param>
    /// <returns>New column after applying the plus operator</returns>
    public StringColumn Plus(StringColumn rhs) => New(Column.Plus(rhs));

    /// <summary>
    /// SQL like expression. Returns a boolean column based on a SQL LIKE match.
    /// </summary>
    /// <param name="literal">The literal that is used to compute the SQL LIKE match</param>
    /// <returns>New column after applying the SQL LIKE match</returns>
    public BooleanColumn Like(string literal) => BooleanColumn.New(Column.Like(literal));

    /// <summary>
    /// SQL RLIKE expression (LIKE with Regex). Returns a boolean column based on a regex
    /// match.
    /// </summary>
    /// <param name="literal">The literal that is used to compute the Regex match</param>
    /// <returns>New column after applying the regex LIKE method</returns>
    public BooleanColumn RLike(string literal) => BooleanColumn.New(Column.RLike(literal));

    /// <summary>An expression that returns a substring.</summary>
    /// <param name="startPos">Expression for the starting position</param>
    /// <param name="len">Expression for the length of the substring</param>
    /// <returns>
    /// New column that is bound by the start position provided, and the length.
    /// </returns>
    public StringColumn SubStr(IntegerColumn startPos, IntegerColumn len) =>
        New(Column.SubStr((Column)startPos, (Column)len));

    /// <summary>
    /// Contains the other element. Returns a boolean column based on a string match.
    /// </summary>
    /// <param name="other">
    /// The object that is used to check for existence in the current column.
    /// </param>
    /// <returns>New column after checking if the column contains object other</returns>
    public BooleanColumn Contains(StringColumn other) =>
        BooleanColumn.New(Column.Contains((Column)other));

    /// <summary>
    /// String starts with. Returns a boolean column based on a string match.
    /// </summary>
    /// <param name="other">
    /// The other column containing strings with which to check how values
    /// in this column starts.
    /// </param>
    /// <returns>
    /// A boolean column where entries are true if values in the current
    /// column does indeed start with the values in the given column.
    /// </returns>
    public BooleanColumn StartsWith(StringColumn other) =>
        BooleanColumn.New(Column.StartsWith((Column)other));

    /// <summary>
    /// String ends with. Returns a boolean column based on a string match.
    /// </summary>
    /// <param name="other">
    /// The other column containing strings with which to check how values
    /// in this column ends.
    /// </param>
    /// <returns>
    /// A boolean column where entries are true if values in the current
    /// column does indeed end with the values in the given column.
    /// </returns>
    public BooleanColumn EndsWith(StringColumn other) =>
        BooleanColumn.New(Column.EndsWith((Column)other));

    /// <summary>
    /// Casts the column to a boolean column, using the canonical string
    /// representation of a boolean.
    /// </summary>
    /// <returns>Column object</returns>
    public BooleanColumn CastToBoolean() => BooleanColumn.New(Column.Cast("boolean"));

    /// <summary>
    /// Casts the column to a integer column, using the canonical string
    /// representation of a integer.
    /// </summary>
    /// <returns>Column object</returns>
    public IntegerColumn CastToInteger() => IntegerColumn.New(Column.Cast("int"));

    /// <summary>
    /// Casts the column to a long column, using the canonical string
    /// representation of a long.
    /// </summary>
    /// <returns>Column object</returns>
    public LongColumn CastToLong() => LongColumn.New(Column.Cast("long"));

    /// <summary>
    /// Casts the column to a double column, using the canonical string
    /// representation of a double.
    /// </summary>
    /// <returns>Column object</returns>
    public DoubleColumn CastToDouble() => DoubleColumn.New(Column.Cast("double"));

    /// <summary>
    /// Casts the column to a date column, using the canonical string
    /// representation of a date.
    /// </summary>
    /// <returns>Column object</returns>
    public DateColumn CastToDate() => DateColumn.New(Column.Cast("date"));

    /// <summary>
    /// Casts the column to a timestamp column, using the canonical string
    /// representation of a timestamp.
    /// </summary>
    /// <returns>Column object</returns>
    public TimestampColumn CastToTimestamp() => TimestampColumn.New(Column.Cast("timestamp"));

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public StringColumn IsIn(string first, params string[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
