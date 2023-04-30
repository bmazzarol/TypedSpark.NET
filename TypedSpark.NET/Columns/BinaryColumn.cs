using System.Diagnostics.CodeAnalysis;
using System.Text;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Binary column
/// </summary>
public sealed class BinaryColumn : TypedOrdColumn<BinaryColumn, BinaryType, byte[]>
{
    private BinaryColumn(Column column)
        : base(new BinaryType(), column) { }

    [ExcludeFromCodeCoverage]
    public BinaryColumn()
        : this(F.Col(string.Empty)) { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>binary column</returns>
    public static BinaryColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the binary column to a string column
    /// </summary>
    /// <returns>binary column</returns>
    public StringColumn Decode(Encoding encoding) =>
        StringColumn.New(
            F.Decode(
                Column,
                encoding switch
                {
                    ASCIIEncoding _ => "US-ASCII",
                    UnicodeEncoding _ => "UTF-16",
                    _ => "UTF-8"
                }
            )
        );

    /// <summary>
    /// Computes the BASE64 encoding of a binary column and returns it as a string column
    /// </summary>
    /// <remarks>This is the reverse of Unbase64</remarks>
    /// <returns>string column</returns>
    public StringColumn Base64() => StringColumn.New(F.Base64(Column));

    /// <summary>
    /// Calculates the MD5 digest of a binary column and returns the value
    /// as a 32 character hex string
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Md5() => StringColumn.New(F.Md5(Column));

    /// <summary>
    /// Calculates the SHA-1 digest of a binary column and returns the value
    /// as a 40 character hex string
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Sha1() => StringColumn.New(F.Sha1(Column));

    /// <summary>
    /// Calculates the SHA-224 hash of a binary column and
    /// returns the value as a hex string
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Sha224() => StringColumn.New(F.Sha2(Column, 224));

    /// <summary>
    /// Calculates the SHA-256 hash of a binary column and
    /// returns the value as a hex string
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Sha256() => StringColumn.New(F.Sha2(Column, 256));

    /// <summary>
    /// Calculates the SHA-384 hash of a binary column and
    /// returns the value as a hex string
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Sha384() => StringColumn.New(F.Sha2(Column, 384));

    /// <summary>
    /// Calculates the SHA-384 hash of a binary column and
    /// returns the value as a hex string
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn Sha512() => StringColumn.New(F.Sha2(Column, 512));

    /// <summary>
    /// Calculates the cyclic redundancy check value (CRC32) of a binary column and
    /// returns the value as a bigint
    /// </summary>
    /// <returns>long column</returns>
    public LongColumn Crc32() => LongColumn.New(F.Crc32(Column));
}
