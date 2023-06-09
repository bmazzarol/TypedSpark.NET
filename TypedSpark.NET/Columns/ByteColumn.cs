﻿using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Byte Column
/// </summary>
/// <remarks>Spark side only, cannot be serialized to Java at this stage</remarks>
public sealed class ByteColumn : TypedIntegralColumn<ByteColumn, ByteType, byte>
{
    private ByteColumn(Column column)
        : base(new ByteType(), column) { }

    /// <summary>
    /// Constructs an empty column
    /// </summary>
    public ByteColumn()
        : this(Col(string.Empty)) { }

    /// <inheritdoc />
    protected internal override object? CoerceToNative() =>
        byte.TryParse(Column.ToString(), out var b) ? b : null;

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static ByteColumn New(string name, Column? column = default) => new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static ByteColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator ByteColumn(byte lit) => New(Lit((int)lit));

    /// <summary>
    /// Cast to string
    /// </summary>
    public static implicit operator StringColumn(ByteColumn column) => column.CastToString();

    /// <summary>
    /// Cast to int
    /// </summary>
    public static implicit operator IntegerColumn(ByteColumn column) => column.CastToInteger();
}
