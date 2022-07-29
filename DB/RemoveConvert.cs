using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace DB
{
    public static class ExpressionExtensions
    {
        public static Expression<TExpressionType> RemoveConvert<TExpressionType>(this Expression<TExpressionType> exp)
        {
            var converter = new RemoveConvertVisitor();
            var newBody = converter.Visit(exp.Body);
            return Expression.Lambda<TExpressionType>(newBody, exp.Parameters);
        }

        private class RemoveConvertVisitor : ExpressionVisitor {
            public override Expression Visit(Expression e) { 
                if(e is UnaryExpression ue)
                {
                    return ue.Operand;
                }

                return base.Visit(e);
            }
        }
    }
}
