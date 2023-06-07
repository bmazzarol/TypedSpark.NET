using System;
using System.Linq;
using Microsoft.Spark.Sql;
using TypedSpark.NET.Columns;

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
    public DataFrame DataFrame { get; }

    /// <summary>
    /// The underlying schema for the data frame
    /// </summary>
    public TSchema Schema { get; }

    private Func<string, TSchema> AliasSchemaBuilder { get; }

    internal TypedDataFrame(
        DataFrame dataFrame,
        TSchema schema,
        Func<string, TSchema> aliasSchemaFn
    )
    {
        Schema = schema;
        DataFrame = dataFrame;
        AliasSchemaBuilder = aliasSchemaFn;
    }

    /// <summary>
    /// Converts a TypedDataFrame to a untyped DataFrame
    /// </summary>
    public static implicit operator DataFrame(TypedDataFrame<TSchema> typedDataFrame) =>
        typedDataFrame.DataFrame;

    /// <summary>Returns a new `DataFrame` with an alias set.</summary>
    /// <param name="alias">Alias name</param>
    /// <returns>Column object</returns>
    public TypedDataFrame<TSchema> As(string alias) =>
        new(DataFrame.As(alias), AliasSchemaBuilder.Invoke(alias), AliasSchemaBuilder);

    /// <summary>
    /// Returns a new `DataFrame` with an alias set. Same as As().
    /// </summary>
    /// <param name="alias">Alias name</param>
    /// <returns>Column object</returns>
    public TypedDataFrame<TSchema> Alias(string alias) => As(alias);

    /// <summary>
    /// Maps the current `DataFrame` to a new `DataFrame`
    /// </summary>
    /// <param name="project">function to project (convert) the current schema to a new one</param>
    /// <typeparam name="T">new schema</typeparam>
    /// <returns>new typed data frame</returns>
    public TypedDataFrame<T> Map<T>(Func<TSchema, T> project)
    {
        var newSchema = project(Schema);
        var columns = newSchema.ExtractColumns(alias: true).ToArray();
        return new(DataFrame.Select(columns), newSchema, _ => newSchema);
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
        return new TypedDataFrame<T>(DataFrame.CrossJoin(ndf), ns, _ => ns);
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
    ) => SelectMany(flatten).Select(x => project(Schema, x));

    /// <summary>
    /// Filters a `DataFrame` using a given boolean column
    /// </summary>
    /// <param name="predicate">predicate function</param>
    /// <returns>new DataFrame filtered by the given boolean column</returns>
    public TypedDataFrame<TSchema> Filter(Func<TSchema, BooleanColumn> predicate) =>
        new(DataFrame.Where((Column)predicate(Schema)), Schema, AliasSchemaBuilder);

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
        new(DataFrame.Union(other.DataFrame), Schema, AliasSchemaBuilder);

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
        new(DataFrame.Intersect(other.DataFrame), Schema, AliasSchemaBuilder);

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
        new(DataFrame.IntersectAll(other.DataFrame), Schema, AliasSchemaBuilder);

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
                .Select(schema.ExtractColumns(true).ToArray()),
            schema,
            _ => schema
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
    /// Returns a new `DataFrame` sorted by the given expressions.
    /// </summary>
    /// <param name="selector">select columns to sort by</param>
    /// <returns>DataFrame object</returns>
    public TypedDataFrame<TSchema> Sort<T>(Func<TSchema, T> selector) =>
        new(
            DataFrame.Sort(selector(Schema).ExtractColumns(false).ToArray()),
            Schema,
            AliasSchemaBuilder
        );

    /// <summary>
    /// Returns a new `DataFrame` sorted by the given expressions.
    /// </summary>
    /// <remarks>This is an alias of the Sort() function.</remarks>
    /// <param name="selector">select columns to sort by</param>
    /// <returns>DataFrame object</returns>
    public TypedDataFrame<TSchema> OrderBy<T>(Func<TSchema, T> selector) => Sort(selector);

    /// <summary>
    /// Returns a new `DataFrame` by taking the first `number` rows.
    /// </summary>
    /// <param name="n">Number of rows to take</param>
    /// <returns>DataFrame object</returns>
    public TypedDataFrame<TSchema> Limit(int n) =>
        new(DataFrame.Limit(n), Schema, AliasSchemaBuilder);
}

/// <summary>
/// Static companion object
/// </summary>
public static class TypedDataFrame
{
    // Review this code if performance becomes an issue
    private static Func<string, T> AliasSchemaFn<T>() =>
        alias =>
            (T)typeof(T).GetConstructor(new[] { typeof(string) })!.Invoke(new[] { (object)alias });

    /// <summary>
    /// Creates a new typed data frame
    /// </summary>
    /// <param name="schema">schema</param>
    /// <param name="dataFrame">data frame</param>
    /// <typeparam name="T">schema type</typeparam>
    /// <returns>new typed data frame</returns>
    public static TypedDataFrame<T> New<T>(T schema, DataFrame dataFrame)
        where T : TypedSchema, new() => new(dataFrame, schema, AliasSchemaFn<T>());

    /// <summary>
    /// Creates a new typed data frame
    /// </summary>
    /// <param name="dataFrame">data frame</param>
    /// <typeparam name="T">schema type</typeparam>
    /// <returns>new typed data frame</returns>
    public static TypedDataFrame<T> New<T>(DataFrame dataFrame)
        where T : TypedSchema, new() => new(dataFrame, new T(), AliasSchemaFn<T>());

    /// <summary>
    /// Creates a new typed data frame
    /// </summary>
    /// <param name="dataFrame">data frame</param>
    /// <param name="schema">schema</param>
    /// <typeparam name="T">schema type</typeparam>
    /// <returns>new typed data frame</returns>
    public static TypedDataFrame<T> AsTyped<T>(this DataFrame dataFrame, T schema)
        where T : TypedSchema, new() => New(schema, dataFrame);

    /// <summary>
    /// Creates a new typed data frame
    /// </summary>
    /// <param name="dataFrame">data frame</param>
    /// <typeparam name="T">schema type</typeparam>
    /// <returns>new typed data frame</returns>
    public static TypedDataFrame<T> AsTyped<T>(this DataFrame dataFrame)
        where T : TypedSchema, new() => New<T>(dataFrame);
}
