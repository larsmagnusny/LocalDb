using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        public long From { get; set; }
        public bool Modified { get; set; }
        public T[] Cache { get; set; }
        public DateTime TimeLoaded { get; set; }
        public int Hits { get; set; }
    }

    public class PageCache<T>
    {
        public PageCache()
        {
            Cache = new Dictionary<long, Page<T>>();
            PageIds = new Queue<long>();
        }

        public T DefaultValue { get; set; }

        public FileStream Stream { get; set; }
        private Dictionary<long, Page<T>> Cache { get; set; }
        private Queue<long> PageIds { get; set; }
        private int TotalPages { get; set; }
        public int MaxPagesInMemory { get; set; }
        public int ItemsPerPage { get; set; }

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

                        SavePage(ref page);
                        Cache.Remove(pageId);
                    }

                    Console.WriteLine($"Removed {count - 1} pages");
                }
            }
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

        public Page<T> CreatePage(long from)
        {
            Page<T> p = new Page<T>
            {
                From = from,
                Modified = true,
                Cache = new T[ItemsPerPage],
                TimeLoaded = DateTime.UtcNow,
                Hits = 0
            };

            if (!DefaultValue.Equals(default(T)))
            {
                for (int i = 0; i < p.Cache.Length; i++)
                {
                    p.Cache[i] = DefaultValue;
                }
            }

            return p;
        }

        public Page<T> LoadPage(long pageId)
        {
            lock (Cache)
            {
                lock (Stream)
                {
                    // first
                    long from = pageId * ItemsPerPage;

                    Page<T> p;

                    if (pageId < TotalPages)
                    {
                        p = ReadPage(from);
                    }

                    p = CreatePage(from);

                    Cache[pageId] = p;
                    PageIds.Enqueue(pageId);

                    return p;
                }
            }
        }

        private Page<T> ReadPage(long from)
        {
            return Stream.Read<Page<T>>(from);
        }

        public void SavePage(ref Page<T> page)
        {
            if (page.Modified)
            {
                Stream.Seek(page.From, SeekOrigin.Begin);
                Stream.Write(page.Cache);
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

                            if (page.Modified)
                                SavePage(ref page);

                            Cache.Remove(pageId);
                        }
                    }
                }
            }
        }
    }

    public struct Slot
    {
        public long hashCode;
        public long prev;
        public long next;
        public long keyPtr;

        public static Slot CreateSlot(long _hashCode, long _prev, long _next, long _keyPtr)
        {
            Slot slot;

            slot.hashCode = _hashCode;
            slot.prev = _prev;
            slot.next = _next;
            slot.keyPtr = _keyPtr;

            return slot;
        }

        public static Slot CreateSlot(byte[] bytes, int offset)
        {
            Slot s;
            int size = 32;
            IntPtr ptr = IntPtr.Zero;
            try
            {
                ptr = Marshal.AllocHGlobal(size);

                Marshal.Copy(bytes, offset, ptr, size);

                s = (Slot)Marshal.PtrToStructure(ptr, typeof(Slot));
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
            return s;
        }

        public byte[] GetBytes()
        {
            int size = Marshal.SizeOf(this);
            byte[] arr = new byte[size];

            IntPtr ptr = IntPtr.Zero;
            try
            {
                ptr = Marshal.AllocHGlobal(size);
                Marshal.StructureToPtr(this, ptr, true);
                Marshal.Copy(ptr, arr, 0, size);
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
            return arr;
        }
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

        private Serializer<Key> _keySerializer;
        private Serializer<Value> _valueSerializer;

        private Thread[] _pageManagerThreads;
        private bool _pageManagerRunning = true;

        public DiskIndex(long capacity = 127)
        {
            _keySerializer = new Serializer<Key>();
            _valueSerializer = new Serializer<Value>();
            _bucketCache = new PageCache<long>();
            _bucketCache.DefaultValue = (long)-1;

            _slotCache = new PageCache<Slot>();
            _keyCache = new PageCache<Key<Key>>();
            _valueCache = new PageCache<Value>();

            _keyType = typeof(Key);
            _valueType = typeof(Value);

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

            for (int i = 0; i < 4; i++)
                _pageManagerThreads[i].Start();

            try
            {
                File.Delete("buckets_00.bin");
                File.Delete("slots_00.bin");
                File.Delete("keys_00.bin");
                File.Delete("values_00.bin");
            }
            catch { }

            _bucketCache.Stream = File.Open("buckets_00.bin", FileMode.OpenOrCreate);
            _slotCache.Stream = File.Open("slots_00.bin", FileMode.OpenOrCreate);
            _keyCache.Stream = File.Open("keys_00.bin", FileMode.OpenOrCreate);
            _valueCache.Stream = File.Open("values_00.bin", FileMode.OpenOrCreate);

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

        private long GetPageId(long filePtr, long cacheSize)
        {
            return filePtr / cacheSize;
        }

        private Value GetValue(long valuePtr)
        {
            var pageId = GetPageId(valuePtr, _valueCacheSize);

            var p = _valueCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[valuePtr - p.From];
        }

        public long AddValue(Value v)
        {
            var valuePtr = _lastValuePtr;

            var pageId = GetPageId(valuePtr, _valueCacheSize);

            var p = _valueCache.GetPage(pageId);

            p.Hits++;

            p.Cache[valuePtr - p.From] = v;

            p.Modified = true;

            _lastValuePtr++;

            return valuePtr;
        }

        private Key<Key> GetKey(long keyPtr)
        {
            var pageId = GetPageId(keyPtr, _keyCacheSize);

            Page<Key<Key>> p = _keyCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[keyPtr - p.From];
        }

        public long AddKey(Key<Key> key)
        {
            var keyPtr = _lastKeyPtr;

            var pageId = GetPageId(keyPtr, _keyCacheSize);

            var p = _keyCache.GetPage(pageId);

            p.Hits++;
            _lastKeyPtr++;

            p.Cache[keyPtr - p.From] = key;

            p.Modified = true;

            return keyPtr;
        }

        private void SetBucket(long bucketPtr, long slotPtr)
        {
            var pageId = GetPageId(bucketPtr, _bucketCacheSize);

            var p = _bucketCache.GetPage(pageId);

            p.Hits++;

            p.Cache[slotPtr - p.From] = slotPtr;
        }

        private long GetBucket(long bucketPtr)
        {
            var pageId = GetPageId(bucketPtr, _bucketCacheSize);

            var p = _bucketCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[bucketPtr - p.From];
        }

        private Slot GetSlot(long slotPtr)
        {
            var pageId = GetPageId(slotPtr, _slotCacheSize);

            var p = _slotCache.GetPage(pageId);

            p.Hits++;

            return p.Cache[slotPtr - p.From];
        }

        private void SetSlot(long slotPtr, Slot slot)
        {
            var pageId = GetPageId(slotPtr, _slotCacheSize);

            var p = _slotCache.GetPage(pageId);

            p.Hits++;

            p.Cache[slotPtr - p.From] = slot;
        }

        private long AddSlot(Slot slot)
        {
            var curSlotPtr = _lastSlotPtr;

            var pageId = GetPageId(curSlotPtr, _slotCacheSize);

            var p = _slotCache.GetPage(pageId);

            p.Hits++;

            p.Cache[curSlotPtr - p.From] = slot;

            _lastSlotPtr++;

            if (slot.next != -1)
            {
                var nextPageId = GetPageId(slot.next, _slotCacheSize);

                var nP = _slotCache.GetPage(nextPageId);

                nP.Hits++;

                nP.Cache[slot.next - nP.From].prev = curSlotPtr;
            }

            if (slot.prev != -1)
            {
                var prevPageId = GetPageId(slot.prev, _slotCacheSize);

                var pP = _slotCache.GetPage(prevPageId);

                pP.Hits++;

                pP.Cache[slot.prev - pP.From].next = curSlotPtr;
            }

            return curSlotPtr;
        }

        private long GetLastSlotPtr(long slotPtr)
        {
            long ret = slotPtr;
            var slot = GetSlot(slotPtr);

            while (slot.next >= 0)
            {
                ret = slot.next;
                slot = GetSlot(slot.next);
            }

            return ret;
        }

        private long GetFirstSlotPtr(long slotPtr)
        {
            long ret = slotPtr;
            var slot = GetSlot(slotPtr);

            while (slot.prev >= 0)
            {
                ret = slot.prev;
                slot = GetSlot(slot.prev);
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
                    var hashCode = (uint)slot.hashCode;
                    var newBucketPtr = (hashCode % newCapacity) * _bucketSize;

                    if (newBucketPtr == bucketPtr)
                        break;

                    var newSlotPtr = GetBucket(newBucketPtr);

                    if (newSlotPtr == -1)
                    {
                        if (slot.next >= 0 && slot.prev >= 0)
                        {
                            SetBucket(bucketPtr, GetFirstSlotPtr(slot.prev));
                            SetBucket(newBucketPtr, slotPtr);

                            var prevSlot = GetSlot(slot.prev);
                            var nextSlot = GetSlot(slot.next);

                            SetSlot(slot.prev, Slot.CreateSlot(prevSlot.hashCode, prevSlot.prev, slot.next, prevSlot.keyPtr));
                            SetSlot(slot.next, Slot.CreateSlot(nextSlot.hashCode, slot.prev, nextSlot.next, nextSlot.keyPtr));
                            SetSlot(slotPtr, Slot.CreateSlot(hashCode, -1, -1, slot.keyPtr));
                        }
                        else if (slot.next >= 0 && slot.prev < 0)
                        {
                            SetBucket(bucketPtr, slot.next);
                            SetBucket(newBucketPtr, slotPtr);

                            var nextSlot = GetSlot(slot.next);

                            SetSlot(slot.next, Slot.CreateSlot(nextSlot.hashCode, -1, nextSlot.next, nextSlot.keyPtr));
                            SetSlot(slotPtr, Slot.CreateSlot(hashCode, -1, -1, slot.keyPtr));
                        }
                        else if (slot.prev >= 0 && slot.next < 0)
                        {
                            SetBucket(bucketPtr, GetFirstSlotPtr(slot.prev));
                            SetBucket(newBucketPtr, slotPtr);

                            var prevSlot = GetSlot(slot.prev);

                            SetSlot(slot.prev, Slot.CreateSlot(prevSlot.hashCode, prevSlot.prev, -1, prevSlot.keyPtr));
                            SetSlot(slotPtr, Slot.CreateSlot(hashCode, -1, -1, slot.keyPtr));
                        }
                        else // Just one item in bucket
                        {
                            SetBucket(bucketPtr, -1);
                            SetBucket(newBucketPtr, slotPtr);

                            SetSlot(slotPtr, Slot.CreateSlot(hashCode, -1, -1, slot.keyPtr));
                        }

                        //TestGetValue(newBucketPtr);
                    }
                    else
                    {
                        if (slot.prev >= 0 && slot.next >= 0)
                        {
                            var firstSlot = GetFirstSlotPtr(slot.prev);

                            SetBucket(bucketPtr, firstSlot);

                            var prevSlot = GetSlot(slot.prev);

                            SetSlot(slot.prev, Slot.CreateSlot(prevSlot.hashCode, prevSlot.prev, slot.next, prevSlot.keyPtr));
                        }
                        else if (slot.next == -1 && slot.prev >= 0)
                        {
                            var firstSlot = GetFirstSlotPtr(slot.prev);

                            SetBucket(bucketPtr, firstSlot);

                            var prevSlot = GetSlot(slot.prev);

                            SetSlot(slot.prev, Slot.CreateSlot(prevSlot.hashCode, prevSlot.prev, -1, prevSlot.keyPtr));
                        }
                        else if (slot.next >= 0 && slot.prev == -1)
                        {
                            SetBucket(bucketPtr, slot.next);
                        }
                        else
                        {
                            SetBucket(bucketPtr, -1);
                        }


                        var lastSlotPtr = GetLastSlotPtr(newSlotPtr);

                        AddSlot(Slot.CreateSlot(hashCode, lastSlotPtr, -1, slot.keyPtr));

                        //TestGetValue(newBucketPtr);
                    }

                    slotPtr = slot.next;
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
                    slotPtr = slot.next;

                    yield return slot;
                } while (slotPtr >= 0);
            }
            else
            {
                do
                {
                    var slot = GetSlot(slotPtr);
                    slotPtr = slot.prev;

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
                var keyPtr = fwdSlot.keyPtr;
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
                var bucketPtr = i * _bucketSize;

                var slotPtr = GetBucket(bucketPtr);

                if (slotPtr == -1)
                    continue;

                var slots = EnumerateSlots(slotPtr);

                foreach (var slot in slots)
                {
                    var key = GetKey(slot.keyPtr);

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
                    var keyResult = GetKey(slot.keyPtr);

                    if (slot.hashCode == hashCode && keyResult.KeyValue.Equals(index))
                    {
                        return GetValue(keyResult.ValuePtr);
                    }

                    slotPtr = slot.next;
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
                    slotPtr = AddSlot(Slot.CreateSlot(hashCode, -1, -1, keyPtr));
                    SetBucket(bucketId, slotPtr);
                    _count++;
                }
                else
                {
                    long lastSlotPtr = GetLastSlotPtr(slotPtr);

                    var valuePtr = AddValue(value);
                    var keyPtr = AddKey(Key<Key>.CreateKey(index, valuePtr));
                    AddSlot(Slot.CreateSlot(hashCode, lastSlotPtr, -1, keyPtr));
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