using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Columns;
using Array = System.Array;

namespace TypedSpark.NET;

/// <summary>
/// A typed schema
/// </summary>
public abstract class TypedSchema
{
    private readonly List<Column> _columns;

    /// <summary>
    /// Spark type this schema represents
    /// </summary>
    public StructType Type { get; set; }

    [SuppressMessage("Design", "MA0051:Method is too long")]
    protected TypedSchema(string? alias, TypedColumn[]? columns = default)
    {
        var t = GetType();

        if (t.GetFields().Length > 0)
            throw new InvalidOperationException("Only properties are supported on a schema");

        var properties = t.GetProperties()
            .Where(x => !string.Equals(x.Name, nameof(Type), StringComparison.Ordinal))
            .ToArray();

        if (properties.Length == 0)
            throw new InvalidOperationException("No properties have been defined on the schema");

        var existingColumns = columns ?? Array.Empty<TypedColumn>();

        if (existingColumns.Length > 0 && existingColumns.Length != properties.Length)
        {
            throw new ArgumentException(
                $"The number of columns provided {existingColumns.Length} need to match the order and number of properties on the schema which is {properties.Length}",
                nameof(columns)
            );
        }

        var tCols = properties
            .Select(
                (p, i) =>
                {
                    if (
                        p.GetMethod?.IsPublic != true
                        || p.SetMethod?.IsPrivate != true
                        || !p.PropertyType.IsSubclassOf(typeof(TypedColumn))
                    )
                    {
                        throw new InvalidOperationException(
                            "All properties must be a TypedColumn with a public getter and private setter"
                        );
                    }

                    var name = p.GetCustomAttribute(typeof(SparkNameAttribute))
                        is SparkNameAttribute sna
                        ? sna.Name
                        : p.Name;

                    var tc =
                        Activator.CreateInstance(p.PropertyType) as TypedColumn
                        ?? throw new InvalidCastException(
                            $"Failed to create {p.PropertyType.Name}"
                        );

                    return (
                        TypedColumn: tc,
                        Field: new StructField(name, tc.ColumnType),
                        Property: p,
                        Index: i
                    );
                }
            )
            .ToList();

        Type = new StructType(tCols.Select(x => x.Field).ToArray());

        _columns = tCols.ConvertAll(tuple =>
        {
            var (typedColumn, field, property, index) = tuple;
            var column =
                existingColumns.ElementAtOrDefault(index)?.Column.As(field.Name) ?? Col(field.Name);
            typedColumn.Column = alias != null ? column.ApplyAlias(alias, ColumnNames()) : column;
            property.SetValue(this, typedColumn);
            return typedColumn.Column;
        });
    }

    public IEnumerable<Column> Columns() => _columns;

    public IEnumerable<string> ColumnNames() => Type.Fields.Select(x => x.Name);
}

public abstract class TypedSchema<T> : TypedSchema
    where T : TypedSchema<T>, new()
{
    protected TypedSchema(string? alias, TypedColumn[]? columns = default)
        : base(alias, columns) { }
}
