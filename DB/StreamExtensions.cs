using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Text;

namespace DB
{
    public static class StreamExtensions
    {
        private static readonly Dictionary<Type, object> _serializerCache = new Dictionary<Type, object>();

        private static readonly byte[] intBuf = new byte[4];

        public static void WriteManaged<T>(this Stream stream, T value)
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

        public static T ReadManaged<T>(this Stream stream)
        {
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



        public static unsafe T ReadUnmanaged<T>(this Stream stream) where T : unmanaged
        {
            T value;
            T* ptr = &value;

            Span<byte> ptrSpan = new(ptr, sizeof(T));

            if (!BitConverter.IsLittleEndian)
                ptrSpan.Reverse();

            stream.Read(ptrSpan);

            return value;
            
        }

        public static unsafe void WriteUnmanaged<T>(this Stream stream, T value) where T : unmanaged
        {
            T* ptr = &value;

            Span<byte> ptrSpan = new(ptr, sizeof(T));

            if (!BitConverter.IsLittleEndian)
                ptrSpan.Reverse();

            stream.Write(ptrSpan);
        }
    }
}
