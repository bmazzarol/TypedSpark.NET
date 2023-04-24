using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using Microsoft.Spark.Sql;
using TypedSpark.NET.Columns;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

internal static class ColumnExtensions
{
    [Pure]
    internal static Column ApplyAlias(
        this Column column,
        string alias,
        IEnumerable<string> columnNames
    )
    {
        var existingDefinition = column.ToString() ?? string.Empty;

        var cns = columnNames.ToList();
        if (cns.Contains(existingDefinition, StringComparer.OrdinalIgnoreCase))
            return Column($"{alias}.{existingDefinition}");

        // remove any existing column alias
        var parts = existingDefinition.Split(
            new[] { " AS " },
            StringSplitOptions.RemoveEmptyEntries
        );
        var columnAlias = parts.Length > 1 ? parts[1] : null;
        // replace any instances of the supported column names where its used
        var expression = cns.OrderByDescending(x => x.Length)
            .Aggregate(
                parts[0],
                (expr, name) =>
                {
                    var idx = expr.IndexOf(name, StringComparison.InvariantCultureIgnoreCase);
                    while (idx > -1)
                    {
                        var rest = expr.Substring(idx);
                        if (idx == 0 || expr[idx - 1] != '.')
                        {
                            expr = $"{expr.Substring(0, idx)}{alias}.{rest}";
                            idx = expr.IndexOf(
                                name,
                                idx + alias.Length + 2,
                                StringComparison.InvariantCultureIgnoreCase
                            );
                        }
                        else
                        {
                            idx = expr.IndexOf(
                                name,
                                idx + 1,
                                StringComparison.InvariantCultureIgnoreCase
                            );
                        }
                    }

                    return expr;
                }
            );

        return Column(columnAlias != null ? $"{expression} AS {columnAlias}" : expression);
    }

    internal static IEnumerable<Column> ExtractColumns<T>(this T schema, bool alias)
    {
        var result = schema switch
        {
            TypedSchema ts => ts.Columns(),
            object o
                => o.GetType()
                    .GetProperties()
                    .Select(p =>
                    {
                        var col =
                            p.GetValue(o) as TypedColumn
                            ?? throw new InvalidOperationException("Only columns are supported");

                        return alias ? ((Column)col).As(p.Name) : col;
                    }),
            _ => Enumerable.Empty<Column>()
        };
        return result;
    }
}
