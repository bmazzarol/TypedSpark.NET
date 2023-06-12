using System.Diagnostics.CodeAnalysis;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Marker interface that indicates the column is a numeric or interval column
/// </summary>
[SuppressMessage("Minor Code Smell", "S101:Types should be named in PascalCase")]
[SuppressMessage("Naming", "CA1715:Identifiers should have correct prefix")]
[SuppressMessage("ReSharper", "InconsistentNaming")]
public interface TypedNumericOrIntervalColumn { }
