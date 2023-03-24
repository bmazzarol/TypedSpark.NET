using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace SparkTest.NET.Extensions;

public static class DataFrameExtensions
{
    /// <summary>
    /// Converts a type to its spark type.
    /// </summary>
    /// <param name="type">.NET type</param>
    /// <param name="serializable">flag to indicate that only types that can be serialized from .NET to Java be supported</param>
    /// <returns>spark data type</returns>
    /// <exception cref="NotSupportedException">if the type is not supported in spark, or not serializable but supported in Spark</exception>
    public static DataType AsSparkType(this Type type, bool serializable)
    {
        while (true)
        {
            if (type.IsGenericType && type.Namespace != null)
            {
                type = type.GenericTypeArguments[0];
                continue;
            }

            // all supported types that can be serialized from dotnet to java
            if (type == typeof(bool))
                return new BooleanType();
            if (type == typeof(int))
                return new IntegerType();
            if (type == typeof(long))
                return new LongType();
            if (type == typeof(double))
                return new DoubleType();
            if (type == typeof(Date))
                return new DateType();
            if (type == typeof(Timestamp))
                return new TimestampType();
            if (type == typeof(string))
                return new StringType();

            if (serializable)
            {
                throw new NotSupportedException(
                    $"{type.FullName} is not supported in Spark or cannot be serialized from .NET to Java at this time"
                );
            }

            if (type == typeof(byte))
                return new ByteType();
            if (type == typeof(short))
                return new ShortType();
            if (type == typeof(decimal))
                return new DecimalType();
            if (type == typeof(float))
                return new FloatType();
            if (type.IsClass)
                return type.AsStructType(serializable);

            throw new NotSupportedException($"{type.FullName} is not supported in Spark");
        }
    }

    /// <summary>
    /// Creates a struct type from a given T
    /// </summary>
    /// <param name="serializable">flag to indicate that all fields of the struct must be serializable from .NET to Java</param>
    /// <typeparam name="T">some T</typeparam>
    /// <returns>struct type</returns>
    /// <exception cref="NotSupportedException">if the type is not supported in spark, or not serializable but supported in Spark</exception>
    [Pure]
    public static StructType CreateStructType<T>(bool serializable) where T : class =>
        typeof(T).AsStructType(serializable);

    /// <summary>
    /// Creates a struct type from a given T
    /// </summary>
    /// <param name="serializable">flag to indicate that all fields of the struct must be serializable from .NET to Java</param>
    /// <typeparam name="T">some T</typeparam>
    /// <returns>struct type</returns>
    /// <exception cref="NotSupportedException">if the type is not supported in spark, or not serializable but supported in Spark</exception>
    [Pure]
    public static StructType AsStructType<T>(this T _, bool serializable) where T : class =>
        CreateStructType<T>(serializable);

    /// <summary>
    /// Creates a struct type from a given Type
    /// </summary>
    /// <param name="type">type to convert</param>
    /// <param name="serializable">flag to indicate that all fields of the struct must be serializable from .NET to Java</param>
    /// <returns>struct type</returns>
    /// <exception cref="NotSupportedException">if the type is not supported in spark, or not serializable but supported in Spark</exception>
    [Pure]
    public static StructType AsStructType(this Type type, bool serializable) =>
        new(
            type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Select(
                    x =>
                        x.PropertyType.IsGenericType
                            ? new StructField(
                                x.Name,
                                x.PropertyType.AsSparkType(serializable),
                                Nullable.GetUnderlyingType(x.PropertyType.GenericTypeArguments[0])
                                    == null
                            )
                            : new StructField(
                                x.Name,
                                x.PropertyType.AsSparkType(serializable),
                                Nullable.GetUnderlyingType(x.PropertyType) == null
                            )
                )
        );

    /// <summary>
    /// Creates a data frame from a given TData
    /// </summary>
    /// <param name="session">session</param>
    /// <param name="first">first data item</param>
    /// <param name="rest">other data items</param>
    /// <typeparam name="TData">some TData reference type</typeparam>
    /// <returns>dataframe</returns>
    /// <exception cref="NotSupportedException">if the type is not supported in spark, or not serializable but supported in Spark</exception>
    [Pure]
    public static DataFrame CreateDataFrameFromData<TData>(
        this SparkSession session,
        TData first,
        params TData[] rest
    ) where TData : class
    {
        var schema = CreateStructType<TData>(true);
        return session.CreateDataFrame(
            new List<TData> { first }
                .Concat(rest)
                .Select(
                    data =>
                        new GenericRow(
                            first
                                .GetType()
                                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                .Select(p => p.GetValue(data))
                                .ToArray()
                        )
                ),
            schema
        );
    }

    /// <summary>
    /// Creates an empty data frame
    /// </summary>
    /// <param name="session">spark session</param>
    /// <returns>empty frame</returns>
    [Pure]
    public static DataFrame CreateEmptyFrame(this SparkSession session) =>
        session.CreateDataFrame(
            new[] { new GenericRow(Array.Empty<object>()) },
            new StructType(Enumerable.Empty<StructField>())
        );
}
