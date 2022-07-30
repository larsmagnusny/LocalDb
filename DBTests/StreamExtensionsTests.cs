using Microsoft.VisualStudio.TestTools.UnitTesting;
using DB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Tests
{
    [TestClass()]
    public class StreamExtensionsTests
    {
        [TestMethod()]
        public void ReadBoolTest()
        {
            byte[] boolBuf = new byte[] { 1 };

            using (var memoryStream = new MemoryStream(boolBuf))
            {

                bool b = memoryStream.ReadBool();

                Assert.IsTrue(b);
            }

            boolBuf = new byte[] { 0 };

            using (var memoryStream = new MemoryStream(boolBuf))
            {
                bool b = memoryStream.ReadBool();

                Assert.IsFalse(b);
            }
        }

        [TestMethod()]
        public void ReadByteTest()
        {
            for (byte b = byte.MinValue; b < byte.MaxValue; b++)
            {
                var buf = new byte[] { b };
                using var memoryStream = new MemoryStream(buf);
                var sb = memoryStream.ReadByteNew();

                Assert.IsTrue(b == sb);
            }
        }

        [TestMethod()]
        public void ReadSByteTest()
        {
            for (sbyte b = sbyte.MinValue; b < sbyte.MaxValue; b++)
            {
                var buf = new byte[] { (byte)b };
                using var memoryStream = new MemoryStream(buf);
                var sb = memoryStream.ReadSByte();

                Assert.IsTrue(b == sb);
            }
        }

        [TestMethod()]
        public void ReadCharTest()
        {
            for (char c = char.MinValue; c < char.MaxValue; c++)
            {
                var buf = BitConverter.GetBytes(c);
                using var memoryStream = new MemoryStream(buf);
                var sc = memoryStream.ReadChar();

                Assert.IsTrue(c == sc);
            }
        }

        [TestMethod()]
        public void ReadDoubleTest()
        {
            var random = new Random(DateTimeOffset.UtcNow.ToUnixTimeSeconds().GetHashCode());

            for (long d = 5000; d < 10000; d++)
            {
                var dd = random.NextDouble() * d;
                var buf = BitConverter.GetBytes(dd);
                using var memoryStream = new MemoryStream(buf);
                var sc = memoryStream.ReadDouble();

                Assert.IsTrue(dd == sc);
            }
        }

        [TestMethod()]
        public void ReadFloatTest()
        {
            var random = new Random(DateTimeOffset.UtcNow.ToUnixTimeSeconds().GetHashCode());

            for (long d = 5000; d < 10000; d++)
            {
                var dd = random.NextSingle() * d;
                var buf = BitConverter.GetBytes(dd);
                using var memoryStream = new MemoryStream(buf);
                var sc = memoryStream.ReadFloat();

                Assert.IsTrue(dd == sc);
            }
        }

        [TestMethod()]
        public void ReadIntTest()
        {
            using var memoryStream = new MemoryStream();

            for (int i = -10000; i < 10000; i++)
            {
                var buf = BitConverter.GetBytes(i);
                memoryStream.Write(buf);
            }

            memoryStream.Seek(0, SeekOrigin.Begin);

            for (int i = -10000; i < 10000; i++)
            {
                Assert.IsTrue(memoryStream.ReadInt() == i);
            }
        }

        [TestMethod()]
        public void ReadUIntTest()
        {
            using var memoryStream = new MemoryStream();

            for (uint i = 0; i < 10000; i++)
            {
                var buf = BitConverter.GetBytes(i);
                memoryStream.Write(buf);
            }

            memoryStream.Seek(0, SeekOrigin.Begin);

            for (uint i = 0; i < 10000; i++)
            {
                Assert.IsTrue(memoryStream.ReadInt() == i);
            }
        }

        [TestMethod()]
        public void ReadLongTest()
        {
            using var memoryStream = new MemoryStream();

            for (long i = -10000; i < 10000; i++)
            {
                var buf = BitConverter.GetBytes(i);
                memoryStream.Write(buf);
            }

            memoryStream.Seek(0, SeekOrigin.Begin);

            for (long i = -10000; i < 10000; i++)
            {
                Assert.IsTrue(memoryStream.ReadLong() == i);
            }
        }

        [TestMethod()]
        public void ReadULongTest()
        {
            using var memoryStream = new MemoryStream();

            for (ulong i = 0; i < 10000; i++)
            {
                var buf = BitConverter.GetBytes(i);
                memoryStream.Write(buf);
            }

            memoryStream.Seek(0, SeekOrigin.Begin);

            for (ulong i = 0; i < 10000; i++)
            {
                Assert.IsTrue(memoryStream.ReadULong() == i);
            }
        }

        [TestMethod()]
        public void ReadShortTest()
        {
            using var memoryStream = new MemoryStream();

            for (short i = short.MinValue; i < short.MaxValue; i++)
            {
                var buf = BitConverter.GetBytes(i);
                memoryStream.Write(buf);
            }

            memoryStream.Seek(0, SeekOrigin.Begin);

            for (short i = short.MinValue; i < short.MaxValue; i++)
            {
                Assert.IsTrue(memoryStream.ReadShort() == i);
            }
        }

        [TestMethod()]
        public void ReadUShortTest()
        {
            using var memoryStream = new MemoryStream();

            for (ushort i = ushort.MinValue; i < ushort.MaxValue; i++)
            {
                var buf = BitConverter.GetBytes(i);
                memoryStream.Write(buf);
            }

            memoryStream.Seek(0, SeekOrigin.Begin);

            for (ushort i = ushort.MinValue; i < ushort.MaxValue; i++)
            {
                Assert.IsTrue(memoryStream.ReadUShort() == i);
            }
        }
    }
}