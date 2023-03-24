using System.Collections.Generic;

namespace TypedSpark.NET.Extensions;

internal static class ArrayExtensions
{
    internal static T[] CombinedWith<T>(this T first, params T[] rest)
    {
        var values = new List<T> { first };
        values.AddRange(rest);
        return values.ToArray();
    }
}
