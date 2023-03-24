using System;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Columns;
using TypedSpark.NET.Extensions;

namespace TypedSpark.NET;

public class TypedDataFrame<TSchema>
{
    protected DataFrame DataFrame { get; }

    public TSchema Schema { get; }

    public TypedDataFrame(DataFrame dataFrame, TSchema schema)
    {
        DataFrame = dataFrame;
        Schema = schema;
        // confirm the schema and dataframe conform
    }

    public static explicit operator DataFrame(TypedDataFrame<TSchema> typedDataFrame) =>
        typedDataFrame.DataFrame;

    public TypedDataFrame<T> Map<T>(Func<TSchema, T> project)
    {
        var newSchema = project(Schema);
        return new(DataFrame.Select(newSchema.ExtractColumns().ToArray()), newSchema);
    }

    public TypedDataFrame<T> Select<T>(Func<TSchema, T> projection) => Map(projection);

    public TypedDataFrame<T> Bind<T>(Func<TSchema, TypedDataFrame<T>> flatten)
    {
        var tdf = flatten(Schema);
        var ns = tdf.Schema;
        var ndf = tdf.DataFrame;
        return new TypedDataFrame<T>(
            DataFrame.CrossJoin(ndf).Select(ns.ExtractColumns().ToArray()),
            ns
        );
    }

    public TypedDataFrame<T> SelectMany<T>(Func<TSchema, TypedDataFrame<T>> flatten) =>
        Bind(flatten);

    public TypedDataFrame<TB> SelectMany<TA, TB>(
        Func<TSchema, TypedDataFrame<TA>> flatten,
        Func<TSchema, TA, TB> project
    ) => Bind(flatten).Select(x => project(Schema, x));

    public TypedDataFrame<TSchema> Filter(Func<TSchema, BooleanColumn> predicate) =>
        new(DataFrame.Where((Column)predicate(Schema)), Schema);

    public TypedDataFrame<TSchema> Where(Func<TSchema, BooleanColumn> predicate) =>
        Filter(predicate);
}

public static class TypedDataFrame
{
    class SchemaA : TypedSchema
    {
        public BooleanColumn A = null!;
        public StringColumn B = null!;

        public SchemaA(StructType type) : base(type) { }
    }

    class SchemaB : TypedSchema
    {
#pragma warning disable S1144
#pragma warning disable CS0414
        public IntegerColumn C = null!;
        public DateColumn D = null!;
#pragma warning restore CS0414
#pragma warning restore S1144

        public SchemaB(StructType type) : base(type) { }
    }

    public static void Test()
    {
        var session = SparkSession.Builder().GetOrCreate();
        var schema1 = new TypedDataFrame<SchemaA>(
            session.CreateDataFrame(
                Enumerable.Empty<GenericRow>(),
                new StructType(Array.Empty<StructField>())
            ),
            new SchemaA(null!)
        );
#pragma warning disable S1481
        var schema2 = new TypedDataFrame<SchemaB>(
            session.CreateDataFrame(
                Enumerable.Empty<GenericRow>(),
                new StructType(Array.Empty<StructField>())
            ),
            new SchemaB(null!)
        );

        schema1.Select(x => (x.A, x.A));

        var result = from a in schema1 from b in schema1 where a.B == b.B select (a.B, b.A);
#pragma warning restore S1481
    }
}
