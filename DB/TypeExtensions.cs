using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DB
{
    public static class TypeExtensions
    {
        private static readonly ConcurrentDictionary<Type, bool> _memoized = new();

        public static bool IsUnmanaged(this Type type)
        {

            // check if we already know the answer
            if (!_memoized.TryGetValue(type, out bool answer))
            {

                if (!type.IsValueType)
                {
                    // not a struct -> false
                    answer = false;
                }
                else if (type.IsPrimitive || type.IsPointer || type.IsEnum)
                {
                    // primitive, pointer or enum -> true
                    answer = true;
                }
                else
                {
                    // otherwise check recursively
                    answer = type
                        .GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                        .All(f => IsUnmanaged(f.FieldType));
                }

                _memoized[type] = answer;
            }

            return answer;
        }
    }
}
