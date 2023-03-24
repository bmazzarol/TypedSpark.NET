using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;

namespace TypedSpark.NET;

public class TypedSchema
{
    public StructType Type { get; }

    protected TypedSchema(StructType type)
    {
        Type = type;
    }

    public static TypedSchema New(params StructField[] fields) => new(new StructType(fields));

    public IEnumerable<Column> Columns() => Type.Fields.Select(x => Col(x.Name));
}
