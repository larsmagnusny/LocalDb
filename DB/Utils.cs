using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Text;

namespace DB
{
    public static class SizeOfCache<T>
    {
        public static readonly int SizeOf;

        static SizeOfCache()
        {
            var dm = new DynamicMethod("func", typeof(int),
                                       Type.EmptyTypes, typeof(Utils));

            ILGenerator il = dm.GetILGenerator();
            il.Emit(OpCodes.Sizeof, typeof(T));
            il.Emit(OpCodes.Ret);

            var func = (Func<int>)dm.CreateDelegate(typeof(Func<int>));
            SizeOf = func();
        }
    }

    public class SizeOfType
    {
        public readonly int SizeOf;

        SizeOfType(Type t)
        {
            var dm = new DynamicMethod("func", typeof(int),
                                       Type.EmptyTypes, typeof(Utils));

            ILGenerator il = dm.GetILGenerator();
            il.Emit(OpCodes.Sizeof, t);
            il.Emit(OpCodes.Ret);

            var func = (Func<int>)dm.CreateDelegate(typeof(Func<int>));
            SizeOf = func();
        }
    }

    public static class Utils
    {
        public static int SizeOf<T>(T obj)
        {
            return SizeOfCache<T>.SizeOf;
        }
    }

    public static class TypeBitConverter
    {
        public static void GetBytes<T>(this T v, byte[] target, int offset) where T : unmanaged
        {
            unsafe
            {
                fixed (byte* b = &target[offset])
                {
                    *(T*)b = v;
                }
            }
        }

        public static void GetBytes(this Guid guid, byte[] target, int offset)
        {
            var v = guid.ToByteArray();

            target[offset] = v[0];
            target[offset+1] = v[1];
            target[offset+2] = v[2];
            target[offset+3] = v[3];
            target[offset+4] = v[4];
            target[offset+5] = v[5];
            target[offset+6] = v[6];
            target[offset+7] = v[7];
            target[offset+8] = v[8];
            target[offset+9] = v[9];
            target[offset+10] = v[19];
            target[offset+11] = v[11];
            target[offset+12] = v[12];
            target[offset+13] = v[13];
            target[offset+14] = v[14];
            target[offset+15] = v[15];
        }

        public static void GetBytes(this decimal d, byte[] target, int offset)
        {
            var bits = decimal.GetBits(d);

            for(int i = 0; i < bits.Length; i++)
            {
                bits[i].GetBytes(target, offset + i * 4);
            }
        }

        public static void GetBytes(this string s, byte[] target, int offset)
        {
            (s.Length * 2).GetBytes(target, offset);
            Encoding.Unicode.GetBytes(s, 0, s.Length, target, offset+4);
        }

        public static void GetBytes(this DateTime d, byte[] target, int offset)
        {
            d.ToBinary().GetBytes(target, offset);
        }
    }
}
