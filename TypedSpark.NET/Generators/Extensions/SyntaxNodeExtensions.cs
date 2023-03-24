using System.Collections.Generic;
using System.Diagnostics.Contracts;
using Microsoft.CodeAnalysis;

namespace TypedSpark.NET.Generators.Extensions;

internal static class SyntaxNodeExtensions
{
    /// <summary>
    /// Returns all the parent nodes from the current node
    /// </summary>
    /// <param name="startNode">some start node</param>
    /// <returns>all parent nodes</returns>
    [Pure]
    public static IEnumerable<SyntaxNode> ParentNodes(this SyntaxNode startNode)
    {
        var currentNode = startNode;
        while (currentNode.Parent is { } p)
        {
            yield return p;
            currentNode = p;
        }
    }
}
