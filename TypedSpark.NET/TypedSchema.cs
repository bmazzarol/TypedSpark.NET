using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;

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

    protected TypedSchema(string? alias)
    {
        var t = GetType();

        if (t.GetFields().Length > 0)
            throw new InvalidOperationException("Only properties are supported on a schema");

        var properties = t.GetProperties()
            .Where(x => !string.Equals(x.Name, nameof(Type), StringComparison.Ordinal))
            .ToArray();

        if (properties.Length == 0)
            throw new InvalidOperationException("No properties have been defined on the schema");

        var tCols = properties
            .Select(p =>
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

                var tc =
                    Activator.CreateInstance(p.PropertyType) as TypedColumn
                    ?? throw new InvalidCastException($"Failed to create {p.PropertyType.Name}");

                return (
                    TypedColumn: tc,
                    Field: new StructField(p.Name, tc.ColumnType),
                    Property: p
                );
            })
            .ToList();

        Type = new StructType(tCols.Select(x => x.Field).ToArray());

        _columns = tCols.ConvertAll(tuple =>
        {
            var (typedColumn, field, property) = tuple;
            typedColumn.Column =
                alias != null ? Col(field.Name).ApplyAlias(alias, ColumnNames()) : Col(field.Name);
            property.SetValue(this, typedColumn);
            return typedColumn.Column;
        });
    }

    public IEnumerable<Column> Columns() => _columns;

    public IEnumerable<string> ColumnNames() => Type.Fields.Select(x => x.Name);
}

public abstract class TypedSchema<T> : TypedSchema where T : TypedSchema<T>, new()
{
    protected TypedSchema(string? alias) : base(alias) { }
}
