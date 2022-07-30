using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DB
{
    public struct Key<T>
    {
        public static Key<T> CreateKey(T keyValue, long valuePtr)
        {
            Key<T> ret = new Key<T>
            {
                KeyValue = keyValue,
                ValuePtr = valuePtr
            };

            return ret;
        }

        public T KeyValue { get; set; }
        public long ValuePtr { get; set; }
    }

    public class Page<T>
    {
        public Page(long id, long from, int itemsPerPage, T? defaultValue = default)
        {
            Id = id;
            From = from;
            Modified = true;
            Cache = new T[itemsPerPage];
            Hits = 0;
            TimeLoaded = DateTime.UtcNow;

            if (defaultValue != null && !defaultValue.Equals(default(T)))
            {
                for (int i = 0; i < Cache.Length; i++)
                {
                    Cache[i] = defaultValue;
                }
            }
        }

        public long Id { get; set; }
        public long From { get; set; }
        public bool Modified { get; set; }
        public T[] Cache { get; set; }
        public DateTime TimeLoaded { get; set; }
        public int Hits { get; set; }

        public virtual void Save(Stream stream)
        {
            if (Modified)
            {
                stream.Seek(From, SeekOrigin.Begin);
                for (long i = 0; i < Cache.Length; i++)
                {
                    stream.WriteManaged(Cache[i]);
                }
            }
        }

        public virtual void LoadFromFile(Stream stream)
        {
            stream.Seek(From, SeekOrigin.Begin);

            for (long i = 0; i < Cache.Length; i++)
            {
                Cache[i] = stream.ReadManaged<T>();
            }
        }
    }

    public class UnmanagedPage<T> : Page<T> where T : unmanaged
    {
        public UnmanagedPage(long id, long from, int itemsPerPage, T defaultValue = default) : base(id, from, itemsPerPage, defaultValue)
        {
        }

        public override void Save(Stream stream)
        {
            if (Modified)
            {
                stream.Seek(From, SeekOrigin.Begin);
                for (long i = 0; i < Cache.Length; i++)
                {
                    stream.WriteUnmanaged(Cache[i]);
                }
            }
        }

        public override void LoadFromFile(Stream stream)
        {
            stream.Seek(From, SeekOrigin.Begin);

            for (long i = 0; i < Cache.Length; i++)
            {
                Cache[i] = stream.ReadUnmanaged<T>();
            }
        }
    }

    public class PageCache<T>
    {
        public PageCache(FileStream stream, bool isUnmanaged = false, T ? defaultValue = default)
        {
            Stream = stream;

            IsUnmanaged = isUnmanaged;
            DefaultValue = defaultValue;
        }

        public FileStream Stream { get; set; }
        private Dictionary<long, Page<T>> Cache { get; set; } = new();
        private Queue<long> PageIds { get; set; } = new();

        private int TotalPages { get; set; }
        public int MaxPagesInMemory { get; set; }
        public int ItemsPerPage { get; set; }
        public T? DefaultValue { get; set; }

        public bool IsUnmanaged { get; set; }

        public bool IsOverflowing()
        {
            return PageIds.Count >= MaxPagesInMemory;
        }

        public void Cleanup()
        {
            lock (Cache)
            {
                int numToRemove = (PageIds.Count - MaxPagesInMemory + 1) + PageIds.Count / 2;

                lock (Stream)
                {
                    var count = 0;
                    while (count++ < numToRemove)
                    {
                        var pageId = PageIds.Dequeue();
                        var page = Cache[pageId];

                        page.Save(Stream);

                        Cache.Remove(pageId);
                    }

                    Console.WriteLine($"Removed {count - 1} pages");
                }
            }
        }

        public long GetPageId(long cachePtr)
        {
            return cachePtr / ItemsPerPage;
        }

        public Page<T> GetPage(long pageId)
        {
            lock (Cache)
            {
                if (Cache.TryGetValue(pageId, out var p))
                {
                    return p;
                }
            }

            return LoadPage(pageId);
        }

        public Page<T> CreatePage(long id, long from)
        {

            Page<T> ret;
            if (!IsUnmanaged)
            {
                return new Page<T>(id, from, ItemsPerPage, DefaultValue);
            }

            var t = typeof(UnmanagedPage<>);

            var tt = t.MakeGenericType(typeof(T));

            return (Page<T>)Activator.CreateInstance(tt, id, from, ItemsPerPage, DefaultValue);
        }

        public Page<T> LoadPage(long pageId)
        {
            lock (Cache)
            {
                // first
                long from = pageId * ItemsPerPage;

                Page<T> p;

                p = CreatePage(pageId, from);

                if (pageId < TotalPages)
                {
                    Stream.Seek(p.From, SeekOrigin.Begin);

                    p.LoadFromFile(Stream);
                }
                else
                {
                    TotalPages++;
                }

                Cache[pageId] = p;
                PageIds.Enqueue(pageId);

                return p;
            }
        }

        public void Flush()
        {
            lock (PageIds)
            {
                lock (Cache)
                {
                    lock (Stream)
                    {
                        while (PageIds.Count > 0)
                        {
                            var pageId = PageIds.Dequeue();
                            var page = Cache[pageId];

                            page.Save(Stream);

                            Cache.Remove(pageId);
                        }
                    }
                }
            }
        }
    }

    public struct Slot
    {
        public long HashCode;
        public long Prev;
        public long Next;
        public long KeyPtr;
    }

    public class DiskIndex<Key, Value> where Key : struct
    {
        private PageCache<long> _bucketCache;
        private PageCache<Slot> _slotCache;
        private PageCache<Key<Key>> _keyCache;
        private PageCache<Value> _valueCache;

        private const int _bucketSize = 8;
        private const int _slotSize = 32;
        private long _lastSlotPtr;

        private long _lastKeyPtr;

        private long _lastValuePtr;

        private long _capacity;
        private long _threshold => _capacity + 1;
        private long _count;

        private byte[] _bucketBuf = new byte[_bucketSize];
        private byte[] _slotBuf = new byte[_slotSize];
        private byte[] _keyBuf;
        private byte[] _valueBuf;

        private int _bucketCacheSize;
        private int _bucketMaxPages;
        private int _slotCacheSize;
        private int _slotMaxPages;
        private int _keySize;
        private int _keyCacheSize;
        private int _keyMaxPages;
        private int _valueSize;
        private int _valueCacheSize;
        private int _valueMaxPages;


        private Type _keyType;
        private Type _valueType;

        //private Serializer<Key> _keySerializer;
        //private Serializer<Value> _valueSerializer;

        private Thread[] _pageManagerThreads;
        private bool _pageManagerRunning = true;

        public DiskIndex(long capacity = 127)
        {
            try
            {
                File.Delete("buckets_00.bin");
                File.Delete("slots_00.bin");
                File.Delete("keys_00.bin");
                File.Delete("values_00.bin");
            }
            catch { }

            //_keySerializer = new Serializer<Key>();
            //_valueSerializer = new Serializer<Value>();

            _keyType = typeof(Key);
            _valueType = typeof(Value);

            _bucketCache = new PageCache<long>(File.Open("buckets_00.bin", FileMode.OpenOrCreate), true, -1);

            _slotCache = new PageCache<Slot>(File.Open("slots_00.bin", FileMode.OpenOrCreate), true);
            _keyCache = new PageCache<Key<Key>>(File.Open("keys_00.bin", FileMode.OpenOrCreate), _keyType.IsUnmanaged());
            _valueCache = new PageCache<Value>(File.Open("values_00.bin", FileMode.OpenOrCreate), _valueType.IsUnmanaged());


            int MaxMemoryPerCache = 128 * 1024 * 1024;

            int cacheSize = 256 * 1024;

            _bucketCacheSize = cacheSize; // 64 KB
            _bucketCache.ItemsPerPage = _bucketCacheSize / _bucketSize;
            _bucketCache.MaxPagesInMemory = MaxMemoryPerCache / _bucketCacheSize;

            _slotCacheSize = cacheSize; // _slotSize * x = 64 * 1024

            while (_slotCacheSize % _slotSize != 0)
                _slotCacheSize++;

            _slotCache.ItemsPerPage = _slotCacheSize / _slotSize;
            _slotCache.MaxPagesInMemory = MaxMemoryPerCache / _slotCacheSize;

            _keySize = SizeOfCache<Key>.SizeOf + 8;
            _keyCacheSize = cacheSize; // Multiple of keySize where KeySize * x = 64 * 1024

            while (_keyCacheSize % _keySize != 0)
                _keyCacheSize++;

            _keyCache.ItemsPerPage = _keyCacheSize / _keySize;
            _keyCache.MaxPagesInMemory = MaxMemoryPerCache / _keyCacheSize;

            _valueSize = SizeOfCache<Value>.SizeOf;
            _valueCacheSize = cacheSize;

            while (_valueCacheSize % _valueSize != 0)
                _valueCacheSize++;

            _valueCache.ItemsPerPage = _valueCacheSize / _valueSize;
            _valueCache.MaxPagesInMemory = MaxMemoryPerCache / _valueCacheSize;

            _count = 0;

            _pageManagerThreads = new Thread[4];
            _pageManagerThreads[0] = new Thread(() => PageManagerThread(_bucketCache));
            _pageManagerThreads[1] = new Thread(() => PageManagerThread(_keyCache));
            _pageManagerThreads[2] = new Thread(() => PageManagerThread(_slotCache));
            _pageManagerThreads[3] = new Thread(() => PageManagerThread(_valueCache));

            for (int i = 0; i < _pageManagerThreads.Length; i++)
                _pageManagerThreads[i].Start();

            Grow(capacity);
        }

        public void PageManagerThread<T1>(PageCache<T1> pageCache)
        {
            while (_pageManagerRunning)
            {
                if (pageCache.IsOverflowing())
                    pageCache.Cleanup();

                while (_pageManagerRunning && !pageCache.IsOverflowing())
                    Thread.Sleep(20);
            }
        }

        

        private Value GetValue(long valuePtr)
        {
            var pageId = _valueCache.GetPageId(valuePtr);

            var p = _valueCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[valuePtr - p.From];
        }

        public long AddValue(Value v)
        {
            var valuePtr = _lastValuePtr;

            var pageId = _valueCache.GetPageId(valuePtr);

            var p = _valueCache.GetPage(pageId);

            p.Hits++;

            p.Cache[valuePtr - p.From] = v;

            p.Modified = true;

            _lastValuePtr++;

            return valuePtr;
        }

        private Key<Key> GetKey(long keyPtr)
        {
            var pageId = _keyCache.GetPageId(keyPtr);

            Page<Key<Key>> p = _keyCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[keyPtr - p.From];
        }

        public long AddKey(Key<Key> key)
        {
            var keyPtr = _lastKeyPtr;

            var pageId = _keyCache.GetPageId(keyPtr);

            var p = _keyCache.GetPage(pageId);

            p.Hits++;
            _lastKeyPtr++;

            p.Cache[keyPtr - p.From] = key;

            p.Modified = true;

            return keyPtr;
        }

        private void SetBucket(long bucketPtr, long slotPtr)
        {
            var pageId = _bucketCache.GetPageId(bucketPtr);

            var p = _bucketCache.GetPage(pageId);

            p.Hits++;
            p.Modified = true;

            p.Cache[slotPtr - p.From] = slotPtr;
        }

        private long GetBucket(long bucketPtr)
        {
            var pageId = _bucketCache.GetPageId(bucketPtr);

            var p = _bucketCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[bucketPtr - p.From];
        }

        private Slot GetSlot(long slotPtr)
        {
            var pageId = _slotCache.GetPageId(slotPtr);

            var p = _slotCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[slotPtr - p.From];
        }

        private void SetSlot(long slotPtr, long hashCode, long prev, long next, long keyPtr)
        {
            var pageId = _slotCache.GetPageId(slotPtr);

            var p = _slotCache.GetPage(pageId);

            p.Hits++;

            Slot s;
            s.HashCode = hashCode;
            s.Prev = prev;
            s.Next = next;
            s.KeyPtr = keyPtr;

            p.Modified = true;

            p.Cache[slotPtr - p.From] = s;
        }

        private long AddSlot(long hashCode, long prev, long next, long keyPtr)
        {
            var curSlotPtr = _lastSlotPtr;

            var pageId = _slotCache.GetPageId(curSlotPtr);

            var p = _slotCache.GetPage(pageId);

            p.Hits++;

            SetSlot(curSlotPtr, hashCode, prev, next, keyPtr);

            _lastSlotPtr++;

            if (next != -1)
            {
                var nextPageId = _slotCache.GetPageId(next);

                var nP = _slotCache.GetPage(nextPageId);

                nP.Hits++;

                nP.Cache[next - nP.From].Prev = curSlotPtr;
            }

            if (prev != -1)
            {
                var prevPageId = _slotCache.GetPageId(prev);

                var pP = _slotCache.GetPage(prevPageId);

                pP.Hits++;

                pP.Cache[prev - pP.From].Next = curSlotPtr;
            }

            p.Modified = true;

            return curSlotPtr;
        }

        private long GetLastSlotPtr(long slotPtr)
        {
            long ret = slotPtr;
            var slot = GetSlot(slotPtr);

            while (slot.Next >= 0)
            {
                ret = slot.Next;
                slot = GetSlot(slot.Next);
            }

            return ret;
        }

        private long GetFirstSlotPtr(long slotPtr)
        {
            long ret = slotPtr;
            var slot = GetSlot(slotPtr);

            while (slot.Prev >= 0)
            {
                ret = slot.Prev;
                slot = GetSlot(slot.Prev);
            }

            return ret;
        }

        private void Grow(long newCapacity)
        {
            var oldCapacity = _capacity;
            _capacity = newCapacity;

            Console.WriteLine($"Growing from {oldCapacity} to {newCapacity}");

            var count = 0;
            for (var i = 0; i < oldCapacity; i++)
            {
                var bucketPtr = i * _bucketSize;

                var slotPtr = GetBucket(bucketPtr);

                if (slotPtr == -1)
                    continue;

                do
                {
                    var slot = GetSlot(slotPtr);
                    var hashCode = (uint)slot.HashCode;
                    var newBucketPtr = (hashCode % newCapacity) * _bucketSize;

                    if (newBucketPtr == bucketPtr)
                        break;

                    var newSlotPtr = GetBucket(newBucketPtr);

                    if (newSlotPtr == -1)
                    {
                        if (slot.Next >= 0 && slot.Prev >= 0)
                        {
                            SetBucket(bucketPtr, GetFirstSlotPtr(slot.Prev));
                            SetBucket(newBucketPtr, slotPtr);

                            var prevSlot = GetSlot(slot.Prev);
                            var nextSlot = GetSlot(slot.Next);

                            SetSlot(slot.Prev, prevSlot.HashCode, prevSlot.Prev, slot.Next, prevSlot.KeyPtr);
                            SetSlot(slot.Next, nextSlot.HashCode, slot.Prev, nextSlot.Next, nextSlot.KeyPtr);
                            SetSlot(slotPtr, hashCode, -1, -1, slot.KeyPtr);
                        }
                        else if (slot.Next >= 0 && slot.Prev < 0)
                        {
                            SetBucket(bucketPtr, slot.Next);
                            SetBucket(newBucketPtr, slotPtr);

                            var nextSlot = GetSlot(slot.Next);

                            SetSlot(slot.Next, nextSlot.HashCode, -1, nextSlot.Next, nextSlot.KeyPtr);
                            SetSlot(slotPtr, hashCode, -1, -1, slot.KeyPtr);
                        }
                        else if (slot.Prev >= 0 && slot.Next < 0)
                        {
                            SetBucket(bucketPtr, GetFirstSlotPtr(slot.Prev));
                            SetBucket(newBucketPtr, slotPtr);

                            var prevSlot = GetSlot(slot.Prev);


                            SetSlot(slot.Prev, prevSlot.HashCode, prevSlot.Prev, -1, prevSlot.KeyPtr);

                            SetSlot(slotPtr, hashCode, -1, -1, slot.KeyPtr);
                        }
                        else // Just one item in bucket
                        {
                            SetBucket(bucketPtr, -1);
                            SetBucket(newBucketPtr, slotPtr);

                            SetSlot(slotPtr, hashCode, -1, -1, slot.KeyPtr);
                        }

                        //TestGetValue(newBucketPtr);
                    }
                    else
                    {
                        if (slot.Prev >= 0 && slot.Next >= 0)
                        {
                            var firstSlot = GetFirstSlotPtr(slot.Prev);

                            SetBucket(bucketPtr, firstSlot);

                            var prevSlot = GetSlot(slot.Prev);

                            SetSlot(slot.Prev, prevSlot.HashCode, prevSlot.Prev, slot.Next, prevSlot.KeyPtr);
                        }
                        else if (slot.Next == -1 && slot.Prev >= 0)
                        {
                            var firstSlot = GetFirstSlotPtr(slot.Prev);

                            SetBucket(bucketPtr, firstSlot);

                            var prevSlot = GetSlot(slot.Prev);

                            SetSlot(slot.Prev, prevSlot.HashCode, prevSlot.Prev, -1, prevSlot.KeyPtr);
                        }
                        else if (slot.Next >= 0 && slot.Prev == -1)
                        {
                            SetBucket(bucketPtr, slot.Next);
                        }
                        else
                        {
                            SetBucket(bucketPtr, -1);
                        }


                        var lastSlotPtr = GetLastSlotPtr(newSlotPtr);

                        AddSlot(hashCode, lastSlotPtr, -1, slot.KeyPtr);

                        slot = default;
                    }

                    slotPtr = slot.Next;
                }
                while (slotPtr >= 0);
            }
        }

        public IEnumerable<Slot> EnumerateSlots(long slotPtr, bool forward = true)
        {
            if (forward)
            {
                do
                {
                    var slot = GetSlot(slotPtr);
                    slotPtr = slot.Next;

                    yield return slot;
                } while (slotPtr >= 0);
            }
            else
            {
                do
                {
                    var slot = GetSlot(slotPtr);
                    slotPtr = slot.Prev;

                    yield return slot;
                } while (slotPtr >= 0);
            }
        }

        public void TestGetValue(long bucketPtr)
        {
            var slotPtr = GetBucket(bucketPtr);

            var fwdSlots = EnumerateSlots(slotPtr);

            foreach (var fwdSlot in fwdSlots)
            {
                var keyPtr = fwdSlot.KeyPtr;
                var key = GetKey(keyPtr);

                var value = GetValue(key.ValuePtr);
            }
        }

        public void Dispose()
        {
            _pageManagerRunning = false;

            for (int i = 0; i < _pageManagerThreads.Length; i++)
            {
                _pageManagerThreads[i].Join();
            }
            Flush();
        }

        public void Flush()
        {
            _bucketCache.Flush();
            _slotCache.Flush();
            _keyCache.Flush();
            _valueCache.Flush();
        }

        public IEnumerable<Key> GetKeys()
        {
            for (int i = 0; i < _capacity; i++)
            {
                var bucketPtr = i;

                var slotPtr = GetBucket(bucketPtr);

                if (slotPtr == -1)
                    continue;

                var slots = EnumerateSlots(slotPtr);

                foreach (var slot in slots)
                {
                    var key = GetKey(slot.KeyPtr);

                    yield return key.KeyValue;
                }
            }
        }

        public Value this[Key index]
        {
            get
            {
                var hashCode = (uint)index.GetHashCode();
                var bucketId = hashCode % _capacity;

                var slotPtr = GetBucket(bucketId);

                if (slotPtr == -1)
                    return default;

                do
                {
                    var slot = GetSlot(slotPtr);
                    var keyResult = GetKey(slot.KeyPtr);

                    if (slot.HashCode == hashCode && keyResult.KeyValue.Equals(index))
                    {
                        return GetValue(keyResult.ValuePtr);
                    }

                    slotPtr = slot.Next;
                } while (slotPtr >= 0);

                return default;
            }

            set
            {
                var hashCode = (uint)index.GetHashCode();
                var bucketId = hashCode % _capacity;

                var slotPtr = GetBucket(bucketId);

                if (slotPtr == -1)
                {
                    var valuePtr = AddValue(value);
                    var keyPtr = AddKey(Key<Key>.CreateKey(index, valuePtr));

                    slotPtr = AddSlot(hashCode, -1, -1, keyPtr);
                    SetBucket(bucketId, slotPtr);
                    _count++;
                }
                else
                {
                    long lastSlotPtr = GetLastSlotPtr(slotPtr);

                    var valuePtr = AddValue(value);
                    var keyPtr = AddKey(Key<Key>.CreateKey(index, valuePtr));
                    AddSlot(hashCode, lastSlotPtr, -1, keyPtr);
                    _count++;
                }

                if (_count >= _threshold)
                {
                    var nextPrime = HashHelpers.GetPrime(_capacity * 2);
                    Grow(nextPrime);
                }
            }
        }
    }
}