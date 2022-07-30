using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace DB
{
    public static class StreamExtensions
    {
        private static readonly Dictionary<Type, object> _serializerCache = new Dictionary<Type, object>();

        private static readonly byte[] intBuf = new byte[4];

        public static void Write<T>(this Stream stream, T value)
        {
            var type = typeof(T);

            ISerializer<T> serializer;
            if (_serializerCache.TryGetValue(typeof(T), out var sObj))
            {
                serializer = (ISerializer<T>)sObj;

            }
            else
            {
                serializer = new Serializer<T>();
            }

            var bytes = serializer.Serialize(value);
            var sizeBytes = BitConverter.GetBytes(bytes.Length);

            if(!type.IsPrimitive)
                stream.Write(sizeBytes, 0, sizeBytes.Length);
            stream.Write(bytes, 0, bytes.Length);
        }

        public static bool ReadBool(this Stream stream)
        {
            unsafe
            {
                bool boolean;
                bool* bPtr = &boolean;

                Span<byte> ptrSpan = new(bPtr, sizeof(bool));

                stream.Read(ptrSpan);

                return boolean;
            }
        }

        public static byte ReadByteNew(this Stream stream)
        {
            unsafe
            {
                byte b;
                byte* bPtr = &b;

                Span<byte> ptrSpan = new(bPtr, sizeof(byte));

                stream.Read(ptrSpan);

                return b;
            }
        }

        public static sbyte ReadSByte(this Stream stream)
        {
            unsafe
            {
                sbyte b;
                sbyte* bPtr = &b;

                Span<byte> ptrSpan = new(bPtr, sizeof(sbyte));

                stream.Read(ptrSpan);

                return b;
            }
        }

        public static char ReadChar(this Stream stream)
        {
            unsafe
            {
                char c;
                char* cPtr = &c;

                Span<byte> ptrSpan = new(cPtr, sizeof(char));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return c;
            }
        }

        public static double ReadDouble(this Stream stream)
        {
            unsafe {
                double d;
                double* dPtr = &d;

                Span<byte> ptrSpan = new(dPtr, sizeof(double));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return d;
            }
        }

        public static double ReadFloat(this Stream stream)
        {
            unsafe
            {
                float f;
                float* fPtr = &f;

                Span<byte> ptrSpan = new(fPtr, sizeof(double));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return f;
            }
        }

        public static int ReadInt(this Stream stream)
        {
            unsafe
            {
                int i;
                int* iPtr = &i;

                Span<byte> ptrSpan = new(iPtr, sizeof(int));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return i;
            }
        }

        public static uint ReadUInt(this Stream stream)
        {
            unsafe
            {
                uint i;
                uint* iPtr = &i;

                Span<byte> ptrSpan = new(iPtr, sizeof(uint));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return i;
            }
        }

        public static long ReadLong(this Stream stream)
        {
            unsafe
            {
                long i;
                long* iPtr = &i;

                Span<byte> ptrSpan = new(iPtr, sizeof(long));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return i;
            }
        }

        public static ulong ReadULong(this Stream stream)
        {
            unsafe
            {
                ulong i;
                ulong* iPtr = &i;

                Span<byte> ptrSpan = new(iPtr, sizeof(ulong));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return i;
            }
        }

        public static short ReadShort(this Stream stream)
        {
            unsafe
            {
                short i;
                short* iPtr = &i;

                Span<byte> ptrSpan = new(iPtr, sizeof(short));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return i;
            }
        }

        public static ushort ReadUShort(this Stream stream)
        {
            unsafe
            {
                ushort i;
                ushort* iPtr = &i;

                Span<byte> ptrSpan = new(iPtr, sizeof(ushort));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Read(ptrSpan);

                return i;
            }
        }

        /* Write */
        public static void WriteBool(this Stream stream, bool value)
        {
            unsafe
            {
                bool* bPtr = &value;

                Span<byte> ptrSpan = new(bPtr, sizeof(bool));

                stream.Write(ptrSpan);
            }
        }

        public static void WriteByteNew(this Stream stream, byte value)
        {
            unsafe
            {
                byte* bPtr = &value;

                Span<byte> ptrSpan = new(bPtr, sizeof(byte));

                stream.Write(ptrSpan);
            }
        }

        public static void WriteSByte(this Stream stream, sbyte value)
        {
            unsafe
            {
                sbyte* bPtr = &value;

                Span<byte> ptrSpan = new(bPtr, sizeof(sbyte));

                stream.Write(ptrSpan);
            }
        }

        public static void WriteChar(this Stream stream, char value)
        {
            unsafe
            {
                char* cPtr = &value;

                Span<byte> ptrSpan = new(cPtr, sizeof(char));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteDouble(this Stream stream, double value)
        {
            unsafe
            {
                double* dPtr = &value;

                Span<byte> ptrSpan = new(dPtr, sizeof(double));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteFloat(this Stream stream, float value)
        {
            unsafe
            {
                float* fPtr = &value;

                Span<byte> ptrSpan = new(fPtr, sizeof(double));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteInt(this Stream stream, int value)
        {
            unsafe
            {
                int* iPtr = &value;

                Span<byte> ptrSpan = new(iPtr, sizeof(int));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteUInt(this Stream stream, uint value)
        {
            unsafe
            {
                uint* iPtr = &value;

                Span<byte> ptrSpan = new(iPtr, sizeof(uint));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteLong(this Stream stream, long value)
        {
            unsafe
            {
                long* iPtr = &value;

                Span<byte> ptrSpan = new(iPtr, sizeof(long));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteULong(this Stream stream, ulong value)
        {
            unsafe
            {
                ulong* iPtr = &value;

                Span<byte> ptrSpan = new(iPtr, sizeof(ulong));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteShort(this Stream stream, short value)
        {
            unsafe
            {
                short* iPtr = &value;

                Span<byte> ptrSpan = new(iPtr, sizeof(short));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static void WriteUShort(this Stream stream, ushort value)
        {
            unsafe
            {
                ushort* iPtr = &value;

                Span<byte> ptrSpan = new(iPtr, sizeof(ushort));

                if (!BitConverter.IsLittleEndian)
                    ptrSpan.Reverse();

                stream.Write(ptrSpan);
            }
        }

        public static T Read<T>(this Stream stream, long position)
        {
            stream.Seek(position, SeekOrigin.Begin);

            ISerializer<T> serializer;
            if (_serializerCache.TryGetValue(typeof(T), out var sObj))
            {
                serializer = (ISerializer<T>)sObj;

            }
            else
            {
                serializer = new Serializer<T>();
            }

            return serializer.Deserialize(stream);
        }
    }
}
