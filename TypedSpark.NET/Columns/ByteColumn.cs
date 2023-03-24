using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Byte Column
/// </summary>
/// <remarks>Spark side only, cannot be serialized to Java at this stage</remarks>
public sealed class ByteColumn : TypedOrdColumn<ByteColumn, ByteType, byte>
{
    private ByteColumn(string columnName, Column column) : base(columnName, new ByteType(), column)
    { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static ByteColumn New(string name, Column? column = default) =>
        new(name, column ?? Col(name));

    protected override ByteColumn New(Column column, string? name = default) =>
        New(name ?? ColumnName, column);

    /// <summary>
    /// Compute bitwise OR of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise OR.
    /// </param>
    /// <returns>New column after applying bitwise OR operator</returns>
    public ByteColumn BitwiseOR(byte other) => New(Column.BitwiseOR(Lit(other)));

    /// <summary>
    /// Compute bitwise OR of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise OR.
    /// </param>
    /// <returns>New column after applying bitwise OR operator</returns>
    public ByteColumn BitwiseOR(ByteColumn other) => New(Column.BitwiseOR(other.Column));

    /// <summary>
    /// Compute bitwise AND of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise AND.
    /// </param>
    /// <returns>New column after applying the bitwise AND operator</returns>
    public ByteColumn BitwiseAND(byte other) => New(Column.BitwiseAND(Lit(other)));

    /// <summary>
    /// Compute bitwise AND of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise AND.
    /// </param>
    /// <returns>New column after applying the bitwise AND operator</returns>
    public ByteColumn BitwiseAND(ByteColumn other) => New(Column.BitwiseAND(other.Column));

    /// <summary>
    /// Compute bitwise XOR of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise XOR.
    /// </param>
    /// <returns>New column after applying bitwise XOR operator</returns>
    public ByteColumn BitwiseXOR(byte other) => New(Column.BitwiseXOR(Lit(other)));

    /// <summary>
    /// Compute bitwise XOR of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise XOR.
    /// </param>
    /// <returns>New column after applying bitwise XOR operator</returns>
    public ByteColumn BitwiseXOR(ByteColumn other) => New(Column.BitwiseXOR(other.Column));

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a byte.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(ColumnName, Column.Cast("string"));

    public static implicit operator StringColumn(ByteColumn column) => column.CastToString();

    /// <summary>
    /// Casts the column to a int column, using the canonical int
    /// representation of a byte.
    /// </summary>
    /// <returns>Column object</returns>
    public IntegerColumn CastToInteger() => IntegerColumn.New(ColumnName, Column.Cast("int"));

    public static implicit operator IntegerColumn(ByteColumn column) => column.CastToInteger();
}
