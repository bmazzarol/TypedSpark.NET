using System;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Columns;
using TypedSpark.NET.Extensions;

namespace TypedSpark.NET;

/// <summary>
/// Provides a typing on a data frame
/// </summary>
/// <typeparam name="TSchema">schema</typeparam>
public sealed class TypedDataFrame<TSchema>
{
    /// <summary>
    /// Underlying data frame
    /// </summary>
    private DataFrame DataFrame { get; }

    /// <summary>
    /// The underlying schema for the data frame
    /// </summary>
    public TSchema Schema { get; }

    internal TypedDataFrame(DataFrame dataFrame, TSchema schema)
    {
        DataFrame = dataFrame;
        Schema = schema;
        // confirm the schema and dataframe conform
    }

    public static explicit operator DataFrame(TypedDataFrame<TSchema> typedDataFrame) =>
        typedDataFrame.DataFrame;

    /// <summary>
    /// Maps the current `DataFrame` to a new `DataFrame`
    /// </summary>
    /// <param name="project">function to project (convert) the current schema to a new one</param>
    /// <typeparam name="T">new schema</typeparam>
    /// <returns>new typed data frame</returns>
    public TypedDataFrame<T> Map<T>(Func<TSchema, T> project)
    {
        var newSchema = project(Schema);
        return new(DataFrame.Select(newSchema.ExtractColumns().ToArray()), newSchema);
    }

    /// <summary>
    /// Maps the current `DataFrame` to a new `DataFrame`
    /// </summary>
    /// <param name="project">function to project (convert) the current schema to a new one</param>
    /// <typeparam name="T">new schema</typeparam>
    /// <returns>new typed data frame</returns>
    public TypedDataFrame<T> Select<T>(Func<TSchema, T> project) => Map(project);

    /// <summary>
    /// <para>Bind (flatmap) projects (Maps) and flattens the resulting `DataFrame`.</para>
    /// <para>This performs a cross join of the current `DataFrame` and resulting new `DataFrame`.</para>
    /// </summary>
    /// <param name="flatten">projects the current schema and returns a new typed data frame with a new schema</param>
    /// <typeparam name="T">new schema</typeparam>
    /// <returns>new typed data frame</returns>
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

    /// <summary>
    /// <para>Bind (flatmap) projects (Maps) and flattens the resulting `DataFrame`.</para>
    /// <para>This performs a cross join of the current `DataFrame` and resulting new `DataFrame`.</para>
    /// </summary>
    /// <param name="flatten">projects the current schema and returns a new typed data frame with a new schema</param>
    /// <typeparam name="T">new schema</typeparam>
    /// <returns>new typed data frame</returns>
    public TypedDataFrame<T> SelectMany<T>(Func<TSchema, TypedDataFrame<T>> flatten) =>
        Bind(flatten);

    /// <summary>
    /// <para>Bind (flatmap) projects (Maps) and flattens the resulting `DataFrame`.</para>
    /// <para>This performs a cross join of the current `DataFrame` and resulting new `DataFrame`.</para>
    /// </summary>
    /// <remarks>required by LINQ, its just a Bind().Map() combination</remarks>
    /// <param name="flatten">projects the current schema and returns a new typed data frame with a new schema</param>
    /// <param name="project">final projection from the new frame to a new schema that considers the first and intermediate schemas</param>
    /// <typeparam name="TA">new schema</typeparam>
    /// <typeparam name="TB">new schema</typeparam>
    /// <returns>new typed data frame</returns>
    public TypedDataFrame<TB> SelectMany<TA, TB>(
        Func<TSchema, TypedDataFrame<TA>> flatten,
        Func<TSchema, TA, TB> project
    ) => Bind(flatten).Select(x => project(Schema, x));

    /// <summary>
    /// Filters a `DataFrame` using a given boolean column
    /// </summary>
    /// <param name="predicate">predicate function</param>
    /// <returns>new DataFrame filtered by the given boolean column</returns>
    public TypedDataFrame<TSchema> Filter(Func<TSchema, BooleanColumn> predicate) =>
        new(DataFrame.Where((Column)predicate(Schema)), Schema);

    /// <summary>
    /// Filters a `DataFrame` using a given boolean column
    /// </summary>
    /// <param name="predicate">predicate function</param>
    /// <returns>new DataFrame filtered by the given boolean column</returns>
    public TypedDataFrame<TSchema> Where(Func<TSchema, BooleanColumn> predicate) =>
        Filter(predicate);

    /// <summary>
    /// Returns a new `DataFrame` containing union of rows in this `DataFrame`
    /// and another `DataFrame`.
    /// </summary>
    /// <param name="other">Other DataFrame</param>
    /// <returns>DataFrame object</returns>
    public TypedDataFrame<TSchema> Union(TypedDataFrame<TSchema> other) =>
        new(DataFrame.Union(other.DataFrame), Schema);

    /// <summary>
    /// Returns a new `DataFrame` containing union of rows in this `DataFrame`
    /// and another `DataFrame`.
    /// </summary>
    /// <returns>DataFrame object</returns>
    public static TypedDataFrame<TSchema> operator &(
        TypedDataFrame<TSchema> lhs,
        TypedDataFrame<TSchema> rhs
    ) => lhs.Union(rhs);

    /// <summary>
    /// Returns a new `DataFrame` containing rows only in both this `DataFrame`
    /// and another `DataFrame`.
    /// </summary>
    /// <remarks>This is equivalent to `INTERSECT` in SQL.</remarks>
    /// <param name="other">Other DataFrame</param>
    /// <returns>DataFrame object</returns>
    public TypedDataFrame<TSchema> Intersect(TypedDataFrame<TSchema> other) =>
        new(DataFrame.Intersect(other.DataFrame), Schema);

    /// <summary>
    /// Returns a new `DataFrame` containing rows only in both this `DataFrame`
    /// and another `DataFrame`.
    /// </summary>
    /// <remarks>This is equivalent to `INTERSECT` in SQL.</remarks>
    /// <returns>DataFrame object</returns>
    public static TypedDataFrame<TSchema> operator |(
        TypedDataFrame<TSchema> lhs,
        TypedDataFrame<TSchema> rhs
    ) => lhs.Intersect(rhs);

    /// <summary>
    /// Returns a new `DataFrame` containing rows only in both this `DataFrame`
    /// and another `DataFrame` while preserving the duplicates.
    /// </summary>
    /// <remarks>This is equivalent to `INTERSECT ALL` in SQL.</remarks>
    /// <param name="other">Other DataFrame</param>
    /// <returns>DataFrame object</returns>
    public TypedDataFrame<TSchema> IntersectAll(TypedDataFrame<TSchema> other) =>
        new(DataFrame.IntersectAll(other.DataFrame), Schema);

    private TypedDataFrame<TB> Join<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project,
        string type
    )
    {
        var schema = project(Schema, other.Schema);
        return new TypedDataFrame<TB>(
            DataFrame
                .Join(other.DataFrame, (Column)join(Schema, other.Schema), type)
                .Select(schema.ExtractColumns()),
            schema
        );
    }

    /// <summary>
    /// Inner join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the inner join of this frame and the other one</returns>
    public TypedDataFrame<TB> InnerJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "inner");

    /// <summary>
    /// Cross join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the cross join of this frame and the other one</returns>
    public TypedDataFrame<TB> CrossJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "cross");

    /// <summary>
    /// Outer join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the outer join of this frame and the other one</returns>
    public TypedDataFrame<TB> OuterJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "outer");

    /// <summary>
    /// Full join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the full join of this frame and the other one</returns>
    public TypedDataFrame<TB> FullJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "full");

    /// <summary>
    /// Full outer join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the full outer join of this frame and the other one</returns>
    public TypedDataFrame<TB> FullOuterJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "full_outer");

    /// <summary>
    /// Left join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the left join of this frame and the other one</returns>
    public TypedDataFrame<TB> LeftJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "left");

    /// <summary>
    /// Left outer join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the left outer join of this frame and the other one</returns>
    public TypedDataFrame<TB> LeftOuterJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "left_outer");

    /// <summary>
    /// Right join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the right join of this frame and the other one</returns>
    public TypedDataFrame<TB> RightJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "right");

    /// <summary>
    /// Right outer join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the right outer join of this frame and the other one</returns>
    public TypedDataFrame<TB> RightOuterJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "right_outer");

    /// <summary>
    /// Left semi join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the left semi join of this frame and the other one</returns>
    public TypedDataFrame<TB> LeftSemiJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "left_semi");

    /// <summary>
    /// Left anti join the other `DataFrame` with this one
    /// </summary>
    /// <param name="other">other data frame</param>
    /// <param name="join">join function</param>
    /// <param name="project">project function</param>
    /// <typeparam name="TA">other schema</typeparam>
    /// <typeparam name="TB">projected schema</typeparam>
    /// <returns>new data frame from the left anti join of this frame and the other one</returns>
    public TypedDataFrame<TB> LeftAntiJoin<TA, TB>(
        TypedDataFrame<TA> other,
        Func<TSchema, TA, BooleanColumn> join,
        Func<TSchema, TA, TB> project
    ) => Join(other, join, project, "left_anti");
}

/// <summary>
/// Static companion object
/// </summary>
public static class TypedDataFrame
{
    /// <summary>
    /// Creates a new typed data frame
    /// </summary>
    /// <param name="schema">schema</param>
    /// <param name="dataFrame">data frame</param>
    /// <typeparam name="T">schema type</typeparam>
    /// <returns>new typed data frame</returns>
    public static TypedDataFrame<T> New<T>(T schema, DataFrame dataFrame) => new(dataFrame, schema);

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
        var schema1 = New(
            new SchemaA(null!),
            session.CreateDataFrame(
                Enumerable.Empty<GenericRow>(),
                new StructType(Array.Empty<StructField>())
            )
        );
#pragma warning disable S1481
        var schema2 = New(
            new SchemaB(null!),
            session.CreateDataFrame(
                Enumerable.Empty<GenericRow>(),
                new StructType(Array.Empty<StructField>())
            )
        );

        schema1.Select(x => (x.A, x.A));

        var result = from a in schema1 from b in schema1 where a.B == b.B select (a.B, b.A);

        var result3 = schema1.InnerJoin(schema2, (a, b) => a.B == b.C, (a, b) => (a.B, b.D));

        var result4 = result.Select(x => (x.B, D: x.B.CastToDate())) & result3;

        var result5 = result.Select(x => (x.B, D: x.B.CastToDate())) | result3;

#pragma warning restore S1481
    }
}
