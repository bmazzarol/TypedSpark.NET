using System;

namespace TypedSpark.NET;

/// <summary>
/// Name of the struct property or column name in spark, can override the property name.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SparkNameAttribute : Attribute
{
    public string Name { get; }

    public SparkNameAttribute(string name) => Name = name;
}
