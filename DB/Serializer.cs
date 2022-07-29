using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace DB
{
    public class Serializer<T>
    {
        private readonly Type _type;
        private readonly Dictionary<string, MethodInfo> _staticMethods;
        private readonly Dictionary<Type, MethodInfo> _getBytes;
        private readonly Dictionary<Type, Func<Expression, Expression, Expression>> _primitiveByteReaders;
        private readonly Dictionary<Type, Func<Expression, Expression, Expression>> _primitiveByteWriters;

        private readonly Dictionary<Type, Func<Expression, Expression, Expression, Expression>> _builtInByteReaders;
        private readonly Dictionary<Type, Func<Expression, Expression, Expression>> _builtInByteWriters;

        public Func<T, byte[]> Serialize { get; private set; }
        public Func<byte[], int, T> Deserialize { get; private set; }

        public Serializer()
        {
            _type = typeof(T);

            var bcType = typeof(BitConverter);

            var bcMethods = bcType.GetMethods(BindingFlags.Public | BindingFlags.Static);

            _getBytes = bcMethods
                .Where(o => o.Name == "GetBytes").ToDictionary(k => k.GetParameters()[0].ParameterType, x => x);

            _staticMethods = new Dictionary<string, MethodInfo>() {
                { "BitConverter.ToBoolean", bcMethods.FirstOrDefault(o => o.Name == "ToBoolean") },
                { "BitConverter.ToChar", bcMethods.FirstOrDefault(o => o.Name == "ToChar") },
                { "BitConverter.ToDouble", bcMethods.FirstOrDefault(o => o.Name == "ToDouble") },
                { "BitConverter.ToSingle", bcMethods.FirstOrDefault(o => o.Name == "ToSingle") },
                { "BitConverter.ToInt32", bcMethods.FirstOrDefault(o => o.Name == "ToInt32") },
                { "BitConverter.ToUInt32", bcMethods.FirstOrDefault(o => o.Name == "ToUInt32") },
                { "BitConverter.ToInt64", bcMethods.FirstOrDefault(o => o.Name == "ToInt64") },
                { "BitConverter.ToUInt64", bcMethods.FirstOrDefault(o => o.Name == "ToUInt64") },
                { "BitConverter.ToInt16", bcMethods.FirstOrDefault(o => o.Name == "ToInt16") },
                { "BitConverter.ToUInt16", bcMethods.FirstOrDefault(o => o.Name == "ToUInt16") }
            };

            _primitiveByteReaders = new Dictionary<Type, Func<Expression, Expression, Expression>>()
            {
                { typeof(bool), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToBoolean"], input, offset) },
                { typeof(byte), (input, offset) => Expression.ArrayAccess(input, offset) },
                { typeof(sbyte), (input, offset) => Expression.Convert(Expression.ArrayAccess(input, offset), typeof(sbyte)) },
                { typeof(char), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToChar"], input, offset) },
                { typeof(double), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToDouble"], input, offset) },
                { typeof(float), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToSingle"], input, offset) },
                { typeof(int), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToInt32"], input, offset) },
                { typeof(uint), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToUInt32"], input, offset) },
                { typeof(long), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToInt64"], input, offset) },
                { typeof(ulong), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToUInt64"], input, offset) },
                { typeof(short), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToInt16"], input, offset) },
                { typeof(ushort), (input, offset) => Expression.Call(_staticMethods["BitConverter.ToUInt16"], input, offset) },
            };

            _primitiveByteWriters = new Dictionary<Type, Func<Expression, Expression, Expression>>()
            {
                { typeof(bool), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(byte), (input, output) => Expression.Assign(output, Expression.NewArrayInit(typeof(byte), input)) },
                { typeof(sbyte), (input, output) => Expression.Assign(output, Expression.NewArrayInit(typeof(byte), Expression.Convert(input, typeof(byte)))) },
                { typeof(char), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(double), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(float), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(int), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(uint), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(long), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(ulong), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(short), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
                { typeof(ushort), (input, output) => Expression.Assign(output, Expression.Call(_getBytes[input.Type], input)) },
            };

            _builtInByteReaders = new Dictionary<Type, Func<Expression, Expression, Expression, Expression>>()
            {
                {
                    typeof(Guid), (input, output, offset) =>
                    {
                        var byteVar = Expression.Variable(typeof(byte[]), "bytes");
                        var assignByteVar = Expression.Assign(
                            byteVar,
                            Expression.NewArrayInit(
                                typeof(byte),
                                Enumerable.Range(0, 16).Select(
                                    o => Expression.ArrayIndex(
                                            input,
                                            Expression.Add(offset, Expression.Constant(o))
                                        )
                                    )
                                )
                            );

                        var constructorInfo = typeof(Guid).GetConstructor(new[]{ typeof(byte[]) });

                        var assignOut = Expression.Assign(output, Expression.New(constructorInfo, byteVar));

                        return Expression.Block(new[]{ byteVar }, assignByteVar, assignOut);
                    }
                },
                {
                    typeof(string), (input, output, offset) =>
                    {
                        var unicodeProperty = typeof(Encoding).GetProperty("Unicode", BindingFlags.Public | BindingFlags.Static);

                        //char[] chars, int index, int count
                        var methodInfo = typeof(Encoding).GetMethod("GetString", new Type[]{ typeof(byte[]), typeof(int), typeof(int) });

                        var propertyAccess = Expression.Property(null, unicodeProperty);

                        return Expression.Assign(output, Expression.Call(propertyAccess, methodInfo, input, Expression.Add(offset, Expression.Constant(4)), Expression.Call(_staticMethods["BitConverter.ToInt32"], input, offset)));
                    }
                },
                {
                    typeof(DateTime), (input, output, offset) =>
                    {
                        var longVar = Expression.Variable(typeof(long), "bin");
                        var assignLongVar = Expression.Assign(longVar, Expression.Call(_staticMethods["BitConverter.ToInt64"], input, offset));

                        var newDtMethod = typeof(DateTime).GetMethod("FromBinary", BindingFlags.Public | BindingFlags.Static);

                        return Expression.Block(new[]{ longVar }, assignLongVar, Expression.Assign(output, Expression.Call(newDtMethod, longVar)));
                    }
                },
                {
                    typeof(decimal), (input, output, offset) =>
                    {
                        //return new decimal(
                        //    BitConverter.ToInt32(bytes, 0),
                        //    BitConverter.ToInt32(bytes, 4),
                        //    BitConverter.ToInt32(bytes, 8),
                        //    bytes[15] == 128,
                        //    bytes[14]);

                        var intType = typeof(int);
                        var boolType = typeof(bool);
                        var byteType = typeof(byte);

                        var constructor = typeof(decimal).GetConstructor(new[]{ intType, intType, intType, boolType, byteType });

                        return Expression.Assign(output, Expression.New(constructor,
                                   Expression.Call(_staticMethods["BitConverter.ToInt32"], input, offset),
                                   Expression.Call(_staticMethods["BitConverter.ToInt32"], input, Expression.Add(offset, Expression.Constant(4))),
                                   Expression.Call(_staticMethods["BitConverter.ToInt32"], input, Expression.Add(offset, Expression.Constant(8))),
                                   Expression.Equal(Expression.ArrayAccess(input, Expression.Add(offset, Expression.Constant(15))), Expression.Constant(128)),
                                   Expression.ArrayAccess(input, Expression.Add(offset, Expression.Constant(14)))
                              ));
                    }
                }
            };

            _builtInByteWriters = new Dictionary<Type, Func<Expression, Expression, Expression>>()
            {
                {
                    typeof(Guid), (input, output) =>
                    {
                        var methodInfo = typeof(Guid).GetMethod("ToByteArray", BindingFlags.Public | BindingFlags.Instance);

                        return Expression.Assign(output, Expression.Call(input, methodInfo, null));
                    }
                },
                {
                    typeof(DateTime), (input, output) =>
                    {
                        var methodInfo = typeof(DateTime).GetMethod("ToBinary", BindingFlags.Public | BindingFlags.Instance);

                        var getBytesMethod = _getBytes[typeof(long)];

                        return Expression.Assign(output, Expression.Call(getBytesMethod, Expression.Call(input, methodInfo, null)));
                    }
                },
                {
                    typeof(string), (input, output) =>
                    {
                        var lenBytesVar = Expression.Variable(typeof(byte[]), "len");
                        var strBytesVar = Expression.Variable(typeof(byte[]), "strBytes");

                        var unicodeProperty = typeof(Encoding).GetProperty("Unicode", BindingFlags.Public | BindingFlags.Static);

                        var methodInfo = typeof(Encoding).GetMethod("GetBytes", new[]{ typeof(string) });

                        var propertyAccess = Expression.Property(null, unicodeProperty);

                        var assignLen = Expression.Assign(lenBytesVar, Expression.Call(_getBytes[typeof(int)], Expression.ArrayLength(strBytesVar)));
                        var assignStrBytes = Expression.Assign(strBytesVar, Expression.Call(propertyAccess, methodInfo, input));
                        var assignRetBytes = Expression.Assign(output, Expression.NewArrayBounds(typeof(byte), Expression.Add(Expression.ArrayLength(strBytesVar), Expression.Constant(4))));

                        var assignLen0 = Expression.Assign(
                            Expression.ArrayAccess(output, Expression.Constant(0)),
                            Expression.ArrayAccess(lenBytesVar, Expression.Constant(0)));

                        var assignLen1 = Expression.Assign(
                            Expression.ArrayAccess(output, Expression.Constant(1)),
                            Expression.ArrayAccess(lenBytesVar, Expression.Constant(1)));

                        var assignLen2 = Expression.Assign(
                            Expression.ArrayAccess(output, Expression.Constant(2)),
                            Expression.ArrayAccess(lenBytesVar, Expression.Constant(2)));

                        var assignLen3 = Expression.Assign(
                            Expression.ArrayAccess(output, Expression.Constant(3)),
                            Expression.ArrayAccess(lenBytesVar, Expression.Constant(3)));

                        // Copy(Array sourceArray, long sourceIndex, Array destinationArray, long destinationIndex, long length);

                        var copyMethod = typeof(Array).GetMethod("Copy", new []{ typeof(Array), typeof(int), typeof(Array), typeof(int), typeof(int) });

                        Expression copyToRet = Expression.Call(copyMethod, strBytesVar, Expression.Constant(0), output, Expression.Constant(4), Expression.ArrayLength(strBytesVar));

                        return Expression.Block(new[]
                            {
                                lenBytesVar,
                                strBytesVar
                            },
                            new[]
                            {
                                assignStrBytes,
                                assignLen,
                                assignRetBytes,
                                assignLen0,
                                assignLen1,
                                assignLen2,
                                assignLen3,
                                copyToRet
                            }
                        );
                    }
                },
                {
                    typeof(decimal), (input, output) =>
                    {
                        //return new decimal(
                        //    BitConverter.ToInt32(bytes, 0),
                        //    BitConverter.ToInt32(bytes, 4),
                        //    BitConverter.ToInt32(bytes, 8),
                        //    bytes[15] == 128,
                        //    bytes[14]);
                        var getBitsMethod = typeof(decimal).GetMethod("GetBits");

                        var variable = Expression.Variable(typeof(int[]), "bits");
                        var countVar = Expression.Variable(typeof(int), "count");
                        var assignCountVar = Expression.Assign(countVar, Expression.Constant(0));

                        var assignRetBytes = Expression.Assign(output, Expression.NewArrayInit(typeof(byte), Enumerable.Range(0, 16).Select(o => Expression.Constant(0))));
                        var assignVariable = Expression.Assign(variable, Expression.Call(getBitsMethod, input));

                        var vb0 = Expression.Variable(typeof(byte[]), "vb0");
                        var vb1 = Expression.Variable(typeof(byte[]), "vb1");
                        var vb2 = Expression.Variable(typeof(byte[]), "vb2");
                        var vb3 = Expression.Variable(typeof(byte[]), "vb3");

                        var vb0Assign = Expression.Assign(vb0, Expression.Call(_getBytes[input.Type], Expression.ArrayAccess(variable, Expression.Constant(0))));
                        var vb1Assign = Expression.Assign(vb0, Expression.Call(_getBytes[input.Type], Expression.ArrayAccess(variable, Expression.Constant(1))));
                        var vb2Assign = Expression.Assign(vb0, Expression.Call(_getBytes[input.Type], Expression.ArrayAccess(variable, Expression.Constant(2))));
                        var vb3Assign = Expression.Assign(vb0, Expression.Call(_getBytes[input.Type], Expression.ArrayAccess(variable, Expression.Constant(3))));

                        var iVar = Expression.Variable(typeof(int), "i");

                        var loopCond = Expression.LessThan(iVar, Expression.Constant(4));


                        var breakLabel = Expression.Label("break");

                        var loopBlock0 = Expression.Block(

                            Expression.IfThen(loopCond, Expression.Break(breakLabel)),
                            Expression.Assign(Expression.ArrayAccess(output, countVar), Expression.ArrayAccess(vb0, iVar)),
                            Expression.PostIncrementAssign(countVar),
                            Expression.PostIncrementAssign(iVar)

                        );

                        var loopBlock1 = Expression.Block(

                            Expression.IfThen(loopCond, Expression.Break(breakLabel)),
                            Expression.Assign(Expression.ArrayAccess(output, countVar), Expression.ArrayAccess(vb1, iVar)),
                            Expression.PostIncrementAssign(countVar),
                            Expression.PostIncrementAssign(iVar)

                        );

                        var loopBlock2 = Expression.Block(

                            Expression.IfThen(loopCond, Expression.Break(breakLabel)),
                            Expression.Assign(Expression.ArrayAccess(output, countVar), Expression.ArrayAccess(vb2, iVar)),
                            Expression.PostIncrementAssign(countVar),
                            Expression.PostIncrementAssign(iVar)

                        );

                        var loopBlock3 = Expression.Block(

                            Expression.IfThen(loopCond, Expression.Break(breakLabel)),
                            Expression.Assign(Expression.ArrayAccess(output, countVar), Expression.ArrayAccess(vb3, iVar)),
                            Expression.PostIncrementAssign(countVar),
                            Expression.PostIncrementAssign(iVar)

                        );

                        var loop0 = Expression.Loop(loopBlock0, breakLabel);
                        var loop1 = Expression.Loop(loopBlock1, breakLabel);
                        var loop2 = Expression.Loop(loopBlock2, breakLabel);
                        var loop3 = Expression.Loop(loopBlock3, breakLabel);

                        var intType = typeof(int);
                        var boolType = typeof(bool);
                        var byteType = typeof(byte);

                        var constructor = typeof(decimal).GetConstructor(new[]{ intType, intType, intType, boolType, byteType });

                        return Expression.Block(new[]{
                            variable, vb0, vb1, vb3
                        },
                        assignRetBytes, assignVariable, vb0Assign, vb1Assign, vb2Assign, vb3Assign, loop0, loop1, loop2, loop3);
                    }
                }
            };

            if (_type == typeof(decimal))
                Debugger.Break();

            var serializer = CreateSerializer();
            var deserializer = CreateDeserializer();

            if(serializer != null)
                Serialize = serializer.Compile();

            if(deserializer != null)
                Deserialize = deserializer.Compile();
        }

        public Expression<Func<T, byte[]>> CreateSerializer()
        {
            var inputParameter = Expression.Parameter(_type, "input");

            var returnLabel = Expression.Label(typeof(byte[]), "return");

            var retVar = Expression.Variable(typeof(byte[]), "ret");

            if (IsNullable(_type, out var ut))
            {

                Expression.Lambda<Func<T, byte[]>>(
                    Expression.Block(
                        retVar,
                        AddNullableWriteWrapper(inputParameter, retVar, _type, ut),
                        Expression.Return(returnLabel, retVar)
                    ), 
                    inputParameter
                );
            }

            return Expression.Lambda<Func<T, byte[]>>(
                Expression.Block(
                    new ParameterExpression[] 
                    { 
                        retVar
                    },
                    CreateWriteExpression(inputParameter, retVar, _type),
                    retVar),
                    inputParameter);
        }

        public Expression<Func<byte[], int, T>> CreateDeserializer()
        {
            var inputParameter = Expression.Parameter(typeof(byte[]), "input");
            var offsetParameter = Expression.Parameter(typeof(int), "offset");

            var outputVar = Expression.Variable(_type, "ret");

            var returnLabel = Expression.Label("return");

            if (IsNullable(_type, out var ut)){
                return Expression.Lambda<Func<byte[], int, T>>(
                    Expression.Block(
                        outputVar,
                        AddNullableReadWrapper(inputParameter, offsetParameter, outputVar, _type, ut),
                        CreateReadExpression(inputParameter, outputVar, offsetParameter, ut)
                    ), inputParameter, offsetParameter
                );
            }

            return Expression.Lambda<Func<byte[], int, T>>(
                Expression.Block(
                    new[] { outputVar },
                    CreateReadExpression(inputParameter, outputVar, offsetParameter, _type)),
                    inputParameter, offsetParameter
            );
        }

        private Expression CreateWriteExpression(Expression inputValue, Expression output, Type t)
        {
            if (t.IsPrimitive)
            {
                // bool	System.Boolean
                // byte System.Byte
                // sbyte System.SByte
                // char System.Char
                // double System.Double
                // float System.Single
                // int System.Int32
                // uint System.UInt32
                // nint    System.IntPtr
                // nuint   System.UIntPtr
                // long System.Int64
                // ulong System.UInt64
                // short System.Int16
                // ushort System.UInt16

                return _primitiveByteWriters[t].Invoke(inputValue, output);
            }

            if (_builtInByteWriters.TryGetValue(t, out var func))
                return func.Invoke(inputValue, output);

            // var properties
            var properties = t.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            List<ParameterExpression> variables = new List<ParameterExpression>();
            List<Expression> expressions = new List<Expression>();

            var retVar = Expression.Variable(typeof(List<byte>), "ret");

            var constructorInfo = typeof(List<byte>).GetConstructor(new Type[] { });
            variables.Add(retVar);

            var assignRetVar = Expression.Assign(retVar, Expression.New(constructorInfo));
            expressions.Add(assignRetVar);

            var iVar = Expression.Variable(typeof(int), "i");
            variables.Add(iVar);

            var arrLenProp = typeof(byte[]).GetProperty("Length");

            var listAddMethod = typeof(List<byte>).GetMethod("Add", BindingFlags.Public | BindingFlags.Instance);
            var listToArayMethod = typeof(List<byte>).GetMethod("ToArray", BindingFlags.Public | BindingFlags.Instance);

            for (var i = 0; i < properties.Length; i++)
            {
                var pt = properties[i].PropertyType;

                var v = Expression.Variable(typeof(byte[]), $"b{i}");

                variables.Add(v);

                var pAccess = Expression.Property(inputValue, properties[i]);

                if (pt.IsPrimitive)
                {
                    expressions.Add(_primitiveByteWriters[pt].Invoke(pAccess, v));
                }
                else if (_builtInByteWriters.TryGetValue(pt, out var f))
                {
                    expressions.Add(f.Invoke(pAccess, v));
                }
                else if (IsNullable(pt, out var ut))
                {
                    expressions.Add(AddNullableWriteWrapper(pAccess, v, pt, ut));
                }

                var loopCond = Expression.GreaterThanOrEqual(iVar, Expression.Property(v, arrLenProp));

                var breakLabel = Expression.Label("break");
                var loopBlock = Expression.Block(

                    Expression.IfThen(loopCond, Expression.Break(breakLabel)),
                    Expression.Call(retVar, listAddMethod, Expression.ArrayAccess(v, iVar)),
                    Expression.PostIncrementAssign(iVar)
                );

                var loopExp = Expression.Loop(loopBlock, breakLabel);

                expressions.Add(Expression.Assign(iVar, Expression.Constant(0)));
                expressions.Add(loopExp);
            }

            expressions.Add(Expression.Assign(output, Expression.Call(retVar, listToArayMethod)));

            return Expression.Block(variables, expressions);
        }

        private Expression CreateReadExpression(Expression input, Expression output, Expression offset, Type t)
        {
            if (t.IsPrimitive)
            {
                // bool	System.Boolean
                // byte System.Byte
                // sbyte System.SByte
                // char System.Char
                // decimal System.Decimal
                // double System.Double
                // float System.Single
                // int System.Int32
                // uint System.UInt32
                // nint    System.IntPtr
                // nuint   System.UIntPtr
                // long System.Int64
                // ulong System.UInt64
                // short System.Int16
                // ushort System.UInt16

                return _primitiveByteReaders[t].Invoke(input, offset);
            }

            if (_builtInByteReaders.TryGetValue(t, out var func))
                return func.Invoke(input, output, offset);

            var variables = new List<ParameterExpression>();
            var expressions = new List<Expression>();

            var properties = _type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.SetField);

            var returnLabel = Expression.Label("return");

            var constructorInfo = _type.GetConstructor(new Type[] { });

            expressions.Add(Expression.Assign(output, Expression.New(constructorInfo)));

            var byteCounterVar = Expression.Variable(typeof(int), "counter");
            variables.Add(byteCounterVar);

            expressions.Add(Expression.Assign(byteCounterVar, Expression.Constant(0)));

            var marshal = typeof(Marshal).GetMethod("SizeOf", new[] { typeof(object) });
            var stringLenProp = typeof(string).GetProperty("Length", BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetField);

            for (int i = 0; i < properties.Length; i++)
            {
                var pt = properties[i].PropertyType;

                if (pt.IsPrimitive)
                {
                    expressions.Add(
                        Expression.Assign(
                            Expression.Property(output, properties[i]),
                            _primitiveByteReaders[pt].Invoke(input, Expression.Add(offset, byteCounterVar))));


                    expressions.Add(
                        Expression.Assign(
                            byteCounterVar,
                            Expression.Add(byteCounterVar, Expression.Constant(Marshal.SizeOf(pt)))
                            ));
                }
                else if(_builtInByteReaders.TryGetValue(pt, out var f))
                {
                    expressions.Add(f.Invoke(input, Expression.Property(output, properties[i]), Expression.Add(offset, byteCounterVar)));

                    if (pt == typeof(string))
                    {
                        expressions.Add(
                            Expression.Assign(
                                byteCounterVar,
                                Expression.Add(
                                    byteCounterVar,
                                    Expression.Add(
                                        Expression.Multiply(
                                            Expression.Property(Expression.Property(output, properties[i]), stringLenProp),
                                            Expression.Constant(2)),
                                        Expression.Constant(4)
                                    )
                                )
                            )
                        );
                    }
                    else
                    {
                        expressions.Add(
                        Expression.Assign(
                            byteCounterVar,
                            Expression.Add(byteCounterVar, Expression.Constant(Marshal.SizeOf(pt)))
                            ));
                    }
                }
                else if(IsNullable(pt, out var ut))
                {
                    expressions.Add(AddNullableReadWrapper(input, Expression.Add(offset, byteCounterVar), Expression.Property(output, properties[i]), pt, ut));

                    
                    expressions.Add(
                        Expression.Assign(
                            byteCounterVar,
                            Expression.Add(byteCounterVar, Expression.Constant(Marshal.SizeOf(ut)))
                            ));
                }
            }

            expressions.Add(output);

            return Expression.Block(variables, expressions);
        }

        private Expression AddNullableWriteWrapper(Expression input, Expression output, Type type, Type underlyingType)
        {
            PropertyInfo hasValueProp = type.GetProperty("HasValue", BindingFlags.Public | BindingFlags.Instance);
            PropertyInfo valueProp = type.GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);

            byte[] nullSignature = Encoding.ASCII.GetBytes("N");

            var typeSize = Marshal.SizeOf(underlyingType);

            Expression elseExpr = Expression.Throw(Expression.Constant("Does not support nullable classes"), typeof(Exception));

            if (underlyingType.IsPrimitive)
            {
                elseExpr = _primitiveByteWriters[underlyingType].Invoke(Expression.Property(input, valueProp), output);
            }
            else if (_builtInByteWriters.TryGetValue(underlyingType, out var f))
            {
                elseExpr = f.Invoke(Expression.Property(input, valueProp), output);
            }

            return Expression.IfThenElse(
                Expression.Not(Expression.Property(input, hasValueProp)),
                Expression.Assign(output, Expression.NewArrayInit(
                    typeof(byte),
                    Enumerable.Range(0, typeSize)
                        .Select(o =>  o < nullSignature.Length ? Expression.Constant(nullSignature[o]) : Expression.Constant((byte)0)))),
                elseExpr
                );
        }

        private Expression AddNullableReadWrapper(Expression input, Expression offset, Expression output, Type type, Type underlyingType)
        {
            var typeSize = Marshal.SizeOf(underlyingType);

            byte[] nullSignature = Encoding.ASCII.GetBytes("N");

            var isNilExpression = Expression.Equal(Expression.ArrayAccess(input, offset), Expression.Constant(nullSignature[0]));

            Expression elseExpr = Expression.Throw(Expression.Constant("Does not support nullable classes"), typeof(Exception));

            if (underlyingType.IsPrimitive)
            {
                elseExpr = Expression.Assign(output, Expression.Convert(_primitiveByteReaders[underlyingType].Invoke(input, offset), type));
            }

            if (_builtInByteReaders.TryGetValue(underlyingType, out var func))
                elseExpr = func.Invoke(input, output, offset);

            return Expression.IfThenElse(
                isNilExpression,
                Expression.Assign(output, Expression.Convert(Expression.Constant(null), type)),
                elseExpr
                );
        }

        private bool IsNullable(Type t, out Type underlyingType)
        {
            underlyingType = Nullable.GetUnderlyingType(t);

            return underlyingType != null;
        }
    }
}
