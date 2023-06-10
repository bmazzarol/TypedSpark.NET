using System;
using System.Diagnostics.CodeAnalysis;

namespace TypedSpark.NET;

/// <summary>
/// Name of the struct property or column name in spark, can override the property name.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
[ExcludeFromCodeCoverage]
public sealed class SparkNameAttribute : Attribute
{
    /// <summary>
    /// Name of the spark column/field
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="name">name</param>
    public SparkNameAttribute(string name) => Name = name;
}
