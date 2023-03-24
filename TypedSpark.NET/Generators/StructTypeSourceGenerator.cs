﻿using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using TypedSpark.NET.Generators.Extensions;

namespace TypedSpark.NET.Generators;

[Generator]
public sealed class StructTypeSourceGenerator : ISourceGenerator
{
    public void Initialize(GeneratorInitializationContext context)
    {
        // no action
    }

    public void Execute(GeneratorExecutionContext context)
    {
        var types =
            from st in context.Compilation.SyntaxTrees
            from n in st.GetRoot(context.CancellationToken).DescendantNodes()
            where n.IsKind(SyntaxKind.ClassDeclaration)
            let c = (ClassDeclarationSyntax)n
            where c.Modifiers.Any(m => m.IsKind(SyntaxKind.PartialKeyword))
            from als in c.AttributeLists
            from a in als.Attributes
            where
                string.Equals(
                    a.Name.ToString(),
                    typeof(GenerateSchemaAttribute).FullName,
                    System.StringComparison.Ordinal
                )
            select (a, c);

        foreach (var (a, c) in types)
        {
            var ns = c.ParentNodes()
                .Where(p => p.IsKind(SyntaxKind.NamespaceDeclaration))
                .Select(n => n.ToString())
                .First();
            var className = c.Identifier.ToString();
            context.AddSource(
                $"{className}.g.cs",
                $@"// <auto-generated/>
using System;

namespace {ns}
{{
    public partial class {className}
    {{
        static void SomeFunction(string name) =>
            Console.WriteLine($""Generator says: Hi from '{{name}}'"");
    }}
}}
"
            );
        }
    }
}
