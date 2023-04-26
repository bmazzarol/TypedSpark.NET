using System;
using System.Linq.Expressions;
using System.Reflection;

namespace TypedSpark.NET;

internal static class ExpressionExtensions
{
    internal static PropertyInfo GetPropertyInfo<TSource, TProperty>(
        this Expression<Func<TSource, TProperty>> propertyLambda
    )
    {
        if (propertyLambda.Body is not MemberExpression member)
        {
            throw new ArgumentException(
                $"Expression '{propertyLambda}' refers to a method, not a property.",
                nameof(propertyLambda)
            );
        }

        if (member.Member is not PropertyInfo propInfo)
        {
            throw new ArgumentException(
                $"Expression '{propertyLambda}' refers to a field, not a property.",
                nameof(propertyLambda)
            );
        }

        return propInfo;
    }
}
