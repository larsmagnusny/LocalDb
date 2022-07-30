using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DB
{
    public interface ISerializer<T>
    {
        byte[] Serialize(T value);
        T Deserialize(Stream stream);
    }
}
