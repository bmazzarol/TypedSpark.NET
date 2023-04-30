﻿using System;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// String column
/// </summary>
public sealed class StringColumn : TypedOrdColumn<StringColumn, StringType, string>
{
    private StringColumn(Column column)
        : base(new StringType(), column) { }

    public StringColumn()
        : this(F.Col(string.Empty)) { }

    protected internal override object? CoerceToNative() => Column.ToString();

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>string column</returns>
    public static StringColumn New(string name, Column? column = default) =>
        new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>string column</returns>
    public static StringColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>string column</returns>
    public static implicit operator StringColumn(string lit) => New(F.Lit(lit));

    public static StringColumn operator +(StringColumn lhs, StringColumn rhs) => lhs.Plus(rhs);

    public static StringColumn operator +(StringColumn lhs, string rhs) => lhs.Plus(rhs);

    public static StringColumn operator +(string lhs, StringColumn rhs) =>
        ((StringColumn)lhs).Plus(rhs);

    /// <summary>Sum of this expression and another expression.</summary>
    /// <param name="rhs">The expression to be summed with</param>
    /// <returns>string column</returns>
    public StringColumn Plus(StringColumn rhs) => New(F.Concat(Column, rhs));

    /// <summary>
    /// Computes the character length of a given string or number of bytes of a binary string
    /// </summary>
    /// <remarks>
    /// The length of character strings includes the trailing spaces. The length of binary
    /// strings includes binary zeros.
    /// </remarks>
    /// <returns>integer column</returns>
    public IntegerColumn Length() => IntegerColumn.New(F.Length(Column));

    /// <summary>
    /// Computes the character length of a given string or number of bytes of a binary string
    /// </summary>
    /// <remarks>
    /// The length of character strings includes the trailing spaces. The length of binary
    /// strings includes binary zeros.
    /// </remarks>
    /// <returns>integer column</returns>
    public IntegerColumn Size() => Length();

    /// <summary>
    /// SQL like expression. Returns a boolean column based on a SQL LIKE match
    /// </summary>
    /// <param name="literal">The literal that is used to compute the SQL LIKE match</param>
    /// <returns>boolean column</returns>
    public BooleanColumn Like(string literal) => BooleanColumn.New(Column.Like(literal));

    /// <summary>
    /// SQL RLIKE expression (LIKE with Regex). Returns a boolean column based on a regex
    /// match.
    /// </summary>
    /// <param name="literal">The literal that is used to compute the Regex match</param>
    /// <returns>boolean column</returns>
    public BooleanColumn RLike(string literal) => BooleanColumn.New(Column.RLike(literal));

    /// <summary>
    /// SQL RLIKE expression (LIKE with Regex). Returns a boolean column based on a regex
    /// match.
    /// </summary>
    /// <param name="literal">The literal that is used to compute the Regex match</param>
    /// <returns>boolean column</returns>
    public BooleanColumn RLike(Regex literal) => RLike(literal.ToString());

    /// <summary>
    /// An expression that returns a substring
    /// </summary>
    /// <param name="startPos">Expression for the starting position</param>
    /// <param name="len">Expression for the length of the substring</param>
    /// <returns>string column</returns>
    public StringColumn SubStr(IntegerColumn startPos, IntegerColumn len) =>
        New(Column.SubStr((Column)startPos, (Column)len));

    /// <summary>
    /// Contains the other element. Returns a boolean column based on a string match.
    /// </summary>
    /// <param name="other">
    /// The object that is used to check for existence in the current column.
    /// </param>
    /// <returns>boolean column</returns>
    public BooleanColumn Contains(StringColumn other) =>
        BooleanColumn.New(Column.Contains((Column)other));

    /// <summary>
    /// String starts with. Returns a boolean column based on a string match.
    /// </summary>
    /// <param name="other">
    /// The other column containing strings with which to check how values
    /// in this column starts.
    /// </param>
    /// <returns>boolean column</returns>
    public BooleanColumn StartsWith(StringColumn other) =>
        BooleanColumn.New(Column.StartsWith((Column)other));

    /// <summary>
    /// String ends with. Returns a boolean column based on a string match.
    /// </summary>
    /// <param name="other">
    /// The other column containing strings with which to check how values
    /// in this column ends.
    /// </param>
    /// <returns>boolean column</returns>
    public BooleanColumn EndsWith(StringColumn other) =>
        BooleanColumn.New(Column.EndsWith((Column)other));

    /// <summary>
    /// Casts the column to a boolean column
    /// </summary>
    /// <returns>boolean column</returns>
    public BooleanColumn CastToBoolean() => BooleanColumn.New(Column.Cast("boolean"));

    /// <summary>
    /// Casts the column to a byte column
    /// </summary>
    /// <returns>byte column</returns>
    public ByteColumn CastToByte() => ByteColumn.New(Column.Cast("byte"));

    /// <summary>
    /// Casts the column to a short column
    /// </summary>
    /// <returns>short column</returns>
    public ShortColumn CastToShort() => ShortColumn.New(Column.Cast("short"));

    /// <summary>
    /// Casts the column to a short column
    /// </summary>
    /// <returns>integer column</returns>
    public IntegerColumn CastToInteger() => IntegerColumn.New(Column.Cast("int"));

    /// <summary>
    /// Casts the column to a long column
    /// </summary>
    /// <returns>long column</returns>
    public LongColumn CastToLong() => LongColumn.New(Column.Cast("long"));

    /// <summary>
    /// Casts the column to a float column
    /// </summary>
    /// <returns>float column</returns>
    public FloatColumn CastToFloat() => FloatColumn.New(Column.Cast("float"));

    /// <summary>
    /// Casts the column to a double column
    /// </summary>
    /// <returns>double column</returns>
    public DoubleColumn CastToDouble() => DoubleColumn.New(Column.Cast("double"));

    /// <summary>
    /// Casts the column to a decimal column
    /// </summary>
    /// <returns>decimal column</returns>
    public DecimalColumn CastToDecimal() => DecimalColumn.New(Column.Cast("decimal"));

    /// <summary>
    /// Casts the column to a date column
    /// </summary>
    /// <returns>date column</returns>
    public DateColumn CastToDate() => DateColumn.New(Column.Cast("date"));

    /// <summary>
    /// Casts the column to a timestamp column
    /// </summary>
    /// <returns>timestamp column</returns>
    public TimestampColumn CastToTimestamp() => TimestampColumn.New(Column.Cast("timestamp"));

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>string column</returns>
    public StringColumn IsIn(string first, params string[] rest) =>
        New(
            F.Expr(
                $"{Column} IN ({string.Join(",", first.CombinedWith(rest).Distinct(StringComparer.Ordinal).Select(x => $"'{x}'"))})"
            )
        );

    /// <summary>
    /// Splits this string into arrays of sentences, where each sentence is an array of words.
    /// </summary>
    /// <returns>array column</returns>
    public ArrayColumn<ArrayColumn<StringColumn>> Sentences() =>
        new() { Column = F.Sentences(Column) };

    /// <summary>
    /// Splits this string into arrays of sentences, where each sentence is an array of words.
    /// </summary>
    /// <param name="language">Language of the locale</param>
    /// <param name="country">Country of the locale</param>
    /// <returns>array column</returns>
    public ArrayColumn<ArrayColumn<StringColumn>> Sentences(
        StringColumn language,
        StringColumn country
    ) => new() { Column = F.Sentences(Column, language, country) };

    /// <summary>
    /// Computes the numeric value of the first character of the string column, and returns
    /// the result as an int column
    /// </summary>
    /// <returns>integer column</returns>
    public IntegerColumn Ascii() => IntegerColumn.New(F.Ascii(Column));

    /// <summary>
    /// Returns a new string column by converting the first letter of each word to uppercase
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn InitCap() => New(F.InitCap(Column));

    /// <summary>
    /// Converts a string column to lower case
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Lower() => New(F.Lower(Column));

    /// <summary>
    /// Converts a string column to upper case
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Upper() => New(F.Upper(Column));

    /// <summary>
    /// Locate the position of the first occurrence of the given substring
    /// starting from the given position offset
    /// </summary>
    /// <remarks>
    /// The position is not zero based, but 1 based index. Returns 0 if the given substring
    /// could not be found.
    /// </remarks>
    /// <param name="substring">Substring to find</param>
    /// <param name="pos">Offset to start the search</param>
    /// <returns>integer column</returns>
    public IntegerColumn Locate(string substring, int pos = 1) =>
        IntegerColumn.New(F.Locate(substring, Column, pos));

    /// <summary>
    /// Left-pad the string column with pad to the given length `len`. If the string column is
    /// longer than `len`, the return value is shortened to `len` characters.
    /// </summary>
    /// <param name="len">Length of padded string</param>
    /// <param name="pad">String used for padding</param>
    /// <returns>string column</returns>
    public StringColumn Lpad(int len, string pad) => New(F.Lpad(Column, len, pad));

    /// <summary>
    /// Trim the spaces from left end for the given string column
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Ltrim() => New(F.Ltrim(Column));

    /// <summary>
    /// Trim the specified character string from left end for the given string column
    /// </summary>
    /// <param name="trimString">String to trim</param>
    /// <returns>string column</returns>
    public StringColumn Ltrim(string trimString) => New(F.Ltrim(Column, trimString));

    /// <summary>
    /// Right-pad the string column with pad to the given length `len`. If the string column is
    /// longer than `len`, the return value is shortened to `len` characters.
    /// </summary>
    /// <param name="len">Length of padded string</param>
    /// <param name="pad">String used for padding</param>
    /// <returns>string column</returns>
    public StringColumn Rpad(int len, string pad) => New(F.Rpad(Column, len, pad));

    /// <summary>
    /// Trim the spaces from right end for the specified string value
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Rtrim() => New(F.Rtrim(Column));

    /// <summary>
    /// Trim the specified character string from right end for the given string column
    /// </summary>
    /// <param name="trimString">String to trim</param>
    /// <returns>string column</returns>
    public StringColumn Rtrim(string trimString) => New(F.Rtrim(Column, trimString));

    /// <summary>
    /// Trim the spaces from left and right end for the specified string value
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Trim() => Ltrim().Rtrim();
}
