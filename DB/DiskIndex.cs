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
    public class Page
    {
        public long From { get; set; }
        public bool Modified { get; set; }
        public byte[] Cache { get; set; }
        public DateTime TimeLoaded { get; set; }
        public int Hits { get; set; }
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

        public static Slot CreateSlot(byte[] bytes, long offset)
        {
            Slot slot;

            slot.hashCode = BitConverter.ToInt64(bytes, (int)(offset));
            slot.prev = BitConverter.ToInt64(bytes, (int)(offset + 8));
            slot.next = BitConverter.ToInt64(bytes, (int)(offset + 16));
            slot.keyPtr = BitConverter.ToInt64(bytes, (int)(offset + 24));

            return slot;
        }

        public byte[] GetBytes()
        {
            byte[] arr = new byte[32];

            int counter = 0;

            var hashCodeBytes = BitConverter.GetBytes(hashCode);
            foreach (var b in hashCodeBytes)
                arr[counter++] = b;

            var prevBytes = BitConverter.GetBytes(prev);
            foreach (var b in prevBytes)
                arr[counter++] = b;

            var nextBytes = BitConverter.GetBytes(next);
            foreach (var b in nextBytes)
                arr[counter++] = b;

            var keyPtrBytes = BitConverter.GetBytes(keyPtr);
            foreach (var b in keyPtrBytes)
                arr[counter++] = b;


            return arr;
        }
    }

    public class DiskIndex<Key, Value> where Key : struct
    {
        private readonly FileStream _bucketStream;
        private Dictionary<long, Page> _bucketCache;
        private LinkedList<long> _bucketPageIds;
        public object bucketLock = new object();

        private readonly FileStream _slotStream;
        private Dictionary<long, Page> _slotCache;
        private LinkedList<long> _slotPageIds;
        public object slotLock = new object();

        private readonly FileStream _keyStream;
        private Dictionary<long, Page> _keyCache;
        private LinkedList<long> _keyPageIds;
        public object keyLock = new object();

        private readonly FileStream _valueStream;
        private Dictionary<long, Page> _valueCache;
        private LinkedList<long> _valuePageIds;
        public object valueLock = new object();

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

        private Thread _pageManagerThread;
        private bool _pageManagerRunning = true;

        public DiskIndex(long capacity = 127)
        {
            _keySerializer = new Serializer<Key>();
            _valueSerializer = new Serializer<Value>();

            _bucketCache = new Dictionary<long, Page>();
            _bucketPageIds = new LinkedList<long>();

            _slotCache = new Dictionary<long, Page>();
            _slotPageIds = new LinkedList<long>();

            _keyCache = new Dictionary<long, Page>();
            _keyPageIds = new LinkedList<long>();

            _valueCache = new Dictionary<long, Page>();
            _valuePageIds = new LinkedList<long>();

            _keyType = typeof(Key);
            _valueType = typeof(Value);

            int MaxMemoryPerCache = 512 * 1024 * 1024;

            int cacheSize = 128 * 1024;

            _bucketCacheSize = cacheSize; // 64 KB
            _bucketMaxPages = MaxMemoryPerCache / _bucketCacheSize;

            _slotCacheSize = cacheSize; // _slotSize * x = 64 * 1024

            while (_slotCacheSize % _slotSize != 0)
                _slotCacheSize++;

            _slotMaxPages = MaxMemoryPerCache / _slotCacheSize;

            _keySize = SizeOfCache<Key>.SizeOf + 8;
            _keyCacheSize = cacheSize; // Multiple of keySize where KeySize * x = 64 * 1024

            while (_keyCacheSize % _keySize != 0)
                _keyCacheSize++;

            _keyMaxPages = MaxMemoryPerCache / _keyCacheSize;

            _valueSize = SizeOfCache<Value>.SizeOf;
            _valueCacheSize = cacheSize;

            while (_valueCacheSize % _valueSize != 0)
                _valueCacheSize++;

            _valueMaxPages = MaxMemoryPerCache / _valueCacheSize;

            _count = 0;

            _pageManagerThread = new Thread(new ThreadStart(PageManagerThread));
            _pageManagerThread.Start();

            try
            {
                File.Delete("buckets_00.bin");
                File.Delete("slots_00.bin");
                File.Delete("keys_00.bin");
                File.Delete("values_00.bin");
            }
            catch { }

            _bucketStream = File.Open("buckets_00.bin", FileMode.OpenOrCreate);
            _slotStream = File.Open("slots_00.bin", FileMode.OpenOrCreate);
            _keyStream = File.Open("keys_00.bin", FileMode.OpenOrCreate);
            _valueStream = File.Open("values_00.bin", FileMode.OpenOrCreate);

            Grow(capacity);
        }

        public Task CleanUpPages(FileStream stream, int maxPages, ref LinkedList<long> pageIds, ref Dictionary<long, Page> pages)
        {
            lock (pages)
            {
                if (pageIds.Count >= maxPages)
                {
                    int numToRemove = (pageIds.Count - maxPages + 1) + pageIds.Count / 2;

                    var priorityBuckets = pageIds
                        .Take(numToRemove).ToArray();

                    lock (stream)
                    {
                        foreach (var itemToRemove in priorityBuckets)
                        {
                            var page = pages[itemToRemove];
                            
                            SavePage(stream, ref page);
                            pages.Remove(itemToRemove);
                            pageIds.Remove(itemToRemove);
                        }

                        Console.WriteLine($"Removed {priorityBuckets.Length} pages");
                    }
                }
            }

            return Task.CompletedTask;
        }

        public async void PageManagerThread()
        {
            while (_pageManagerRunning)
            {
                Task[] cleanupTasks = new Task[] {
                    Task.Run(() => CleanUpPages(_bucketStream, _bucketMaxPages, ref _bucketPageIds, ref _bucketCache)),
                    Task.Run(() => CleanUpPages(_keyStream, _keyMaxPages, ref _keyPageIds, ref _keyCache)),
                    Task.Run(() => CleanUpPages(_slotStream, _slotMaxPages, ref _slotPageIds, ref _slotCache)),
                    Task.Run(() => CleanUpPages(_valueStream, _valueMaxPages, ref _valuePageIds, ref _valueCache))
                };

                Task.WaitAll(cleanupTasks);

                GC.Collect();

                Thread.Sleep(2000);
            }
        }

        private long GetPageId(long filePtr, long cacheSize)
        {
            return filePtr / cacheSize;
        }

        private void SavePage(FileStream stream, ref Page p)
        {
            if (p.Modified)
            {
                stream.Seek(p.From, SeekOrigin.Begin);

                stream.Write(p.Cache, 0, p.Cache.Length);
            }
        }

        private Page CreatePage(long from, long cacheSize)
        {
            Page p = new Page
            {
                From = from,
                Modified = true,
                Cache = new byte[cacheSize],
                TimeLoaded = DateTime.UtcNow,
                Hits = 0
            };

            var cache = new Span<byte>(p.Cache);

            cache.Fill(255);

            return p;
        }

        private Page LoadPage(FileStream stream, long pageId, long cacheSize, ref Dictionary<long, Page> pages, ref LinkedList<long> pageIds, int loadForward = 100)
        {
            Page ret = null;

            lock (pages)
            {
                lock (stream)
                {
                    // first
                    long from = pageId * cacheSize;

                    Page p = CreatePage(from, cacheSize);

                    if (from < stream.Length)
                    {
                        stream.Seek(from, SeekOrigin.Begin);
                        stream.Read(p.Cache, 0, p.Cache.Length);
                    }

                    pages[pageId] = p;
                    pageIds.AddLast(pageId);

                    ret = p;

                    for (long pId = pageId + 1; pId < pageId + loadForward; pId++)
                    {
                        if (pages.ContainsKey(pId))
                            continue;

                        from = pId * cacheSize;

                        p = CreatePage(from, cacheSize);

                        if(from < stream.Length)
                        {
                            p.Modified = false;
                            stream.Seek(from, SeekOrigin.Begin);
                            stream.Read(p.Cache, 0, p.Cache.Length);
                        }

                        pages[pId] = p;
                        pageIds.AddLast(pId);
                    }

                    return ret;
                }
            }
        }

        private Value GetValue(long valuePtr)
        {
            var pageId = GetPageId(valuePtr, _valueCacheSize);

            Page p;

            lock (_valueCache)
            {
                if (!_valueCache.TryGetValue(pageId, out p))
                {
                    p = LoadPage(_valueStream, pageId, _valueCacheSize, ref _valueCache, ref _valuePageIds);
                }

                p.Hits++;

                var offset = (int)(valuePtr - p.From);

                return _valueSerializer.Deserialize(p.Cache, offset);
            }
        }

        public long AddValue(Value v)
        {
            var valuePtr = _lastValuePtr;

            var pageId = GetPageId(valuePtr, _valueCacheSize);

            var bytes = _valueSerializer.Serialize(v);

            lock (_valueCache)
            {
                if (!_valueCache.TryGetValue(pageId, out Page p))
                {
                    p = LoadPage(_valueStream, pageId, _valueCacheSize, ref _valueCache, ref _valuePageIds);
                }

                p.Hits++;

                var offset = (int)(valuePtr - p.From);

                for (int i = 0; i < bytes.Length; i++)
                {
                    p.Cache[i + offset] = bytes[i];
                }

                p.Modified = true;
            }

            _lastValuePtr += bytes.Length;

            return valuePtr;
        }

        private (Key key, long valuePtr) GetKey(long keyPtr)
        {
            var pageId = GetPageId(keyPtr, _keyCacheSize);

            Page p;

            lock (_keyCache)
            {
                if (!_keyCache.TryGetValue(pageId, out p))
                {
                    p = LoadPage(_keyStream, pageId, _keyCacheSize, ref _keyCache, ref _keyPageIds);
                }

                p.Hits++;

                var offset = (int)(keyPtr - p.From);

                var valuePtr = BitConverter.ToInt64(p.Cache, offset);

                return (_keySerializer.Deserialize(p.Cache, offset + 8), valuePtr);
            }
        }

        public long AddKey(Key k, long valuePtr)
        {
            var keyPtr = _lastKeyPtr;

            var pageId = GetPageId(keyPtr, _keyCacheSize);

            var bytes = _keySerializer.Serialize(k);

            lock (_keyCache)
            {
                if (!_keyCache.TryGetValue(pageId, out Page p))
                {
                    p = LoadPage(_keyStream, pageId, _keyCacheSize, ref _keyCache, ref _keyPageIds);
                }

                p.Hits++;

                var offset = (int)(keyPtr - p.From);

                valuePtr.GetBytes(p.Cache, offset);

                offset += 8;

                for (int i = 0; i < bytes.Length; i++)
                    p.Cache[offset + i] = bytes[i];

                p.Modified = true;
            }

            _lastKeyPtr += _keySize;

            return keyPtr;
        }

        private void WriteCache(ref Page p, int offset, byte[] bytes)
        {
            for (int i = 0; i < bytes.Length; i++)
            {
                p.Cache[i + offset] = bytes[i];
            }

            p.Modified = true;
        }

        private void SetBucket(long bucketPtr, long slotPtr)
        {
            var bytes = BitConverter.GetBytes(slotPtr);

            var pageId = GetPageId(bucketPtr, _bucketCacheSize);

            Page p;

            lock (_bucketCache)
            {
                if (!_bucketCache.TryGetValue(pageId, out p))
                {
                    p = LoadPage(_bucketStream, pageId, _bucketCacheSize, ref _bucketCache, ref _bucketPageIds);
                }

                p.Hits++;

                WriteCache(ref p, (int)(bucketPtr - p.From), bytes);
            }
        }

        private long GetBucket(long bucketPtr)
        {
            var pageId = GetPageId(bucketPtr, _bucketCacheSize);

            lock (_bucketCache)
            {
                if (!_bucketCache.TryGetValue(pageId, out var p))
                {
                    p = LoadPage(_bucketStream, pageId, _bucketCacheSize, ref _bucketCache, ref _bucketPageIds);
                }

                p.Hits++;

                var offset = (int)(bucketPtr - p.From);

                return BitConverter.ToInt64(p.Cache, offset);
            }
        }

        private Slot GetSlot(long slotPtr)
        {
            var pageId = GetPageId(slotPtr, _slotCacheSize);

            Page p;

            lock (_slotCache)
            {
                if (!_slotCache.TryGetValue(pageId, out p))
                {
                    p = LoadPage(_slotStream, pageId, _slotCacheSize, ref _slotCache, ref _slotPageIds);
                }

                p.Hits++;

                var offset = slotPtr - p.From;

                return Slot.CreateSlot(p.Cache, offset);
            }
        }

        private void SetSlot(long slotPtr, Slot slot)
        {
            var pageId = GetPageId(slotPtr, _slotCacheSize);

            var bytes = slot.GetBytes();

            Page p;


            lock (_slotCache)
            {
                if (!_slotCache.TryGetValue(pageId, out p))
                {
                    p = LoadPage(_slotStream, pageId, _slotCacheSize, ref _slotCache, ref _slotPageIds);
                }

                p.Hits++;

                WriteCache(ref p, (int)(slotPtr - p.From), bytes);
            }
        }

        private long AddSlot(Slot slot)
        {
            var bytes = slot.GetBytes();

            var curSlotPtr = _lastSlotPtr;

            var curSlotBytes = BitConverter.GetBytes(curSlotPtr);

            var pageId = GetPageId(curSlotPtr, _slotCacheSize);

            lock (_slotCache)
            {
                if (!_slotCache.TryGetValue(pageId, out var page))
                {
                    page = LoadPage(_slotStream, pageId, _slotCacheSize, ref _slotCache, ref _slotPageIds);
                }

                page.Hits++;

                WriteCache(ref page, (int)(curSlotPtr - page.From), bytes);

                _lastSlotPtr += bytes.Length;

                if (slot.next != -1)
                {
                    var nextPageId = GetPageId(slot.next, _slotCacheSize);

                    if (!_slotCache.TryGetValue(nextPageId, out var nPage))
                    {

                        nPage = LoadPage(_slotStream, nextPageId, _slotCacheSize, ref _slotCache, ref _slotPageIds);
                    }

                    nPage.Hits++;

                    var offset = (int)(slot.next - nPage.From) + 8;

                    WriteCache(ref nPage, offset, curSlotBytes);
                }

                if (slot.prev != -1)
                {
                    var prevPageId = GetPageId(slot.prev, _slotCacheSize);

                    if (!_slotCache.TryGetValue(prevPageId, out var pPage))
                    {
                        pPage = LoadPage(_slotStream, prevPageId, _slotCacheSize, ref _slotCache, ref _slotPageIds);
                    }

                    pPage.Hits++;

                    var offset = (int)(slot.prev - pPage.From) + 16;

                    WriteCache(ref pPage, offset, curSlotBytes);
                }
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
            for(var i = 0; i < oldCapacity; i++)
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
                        if(slot.next >= 0 && slot.prev >= 0)
                        {
                            SetBucket(bucketPtr, GetFirstSlotPtr(slot.prev));
                            SetBucket(newBucketPtr, slotPtr);

                            var prevSlot = GetSlot(slot.prev);
                            var nextSlot = GetSlot(slot.next);

                            SetSlot(slot.prev, Slot.CreateSlot(prevSlot.hashCode, prevSlot.prev, slot.next, prevSlot.keyPtr));
                            SetSlot(slot.next, Slot.CreateSlot(nextSlot.hashCode, slot.prev, nextSlot.next, nextSlot.keyPtr));
                            SetSlot(slotPtr, Slot.CreateSlot(hashCode, -1, -1, slot.keyPtr));
                        }
                        else if(slot.next >= 0 && slot.prev < 0)
                        {
                            SetBucket(bucketPtr, slot.next);
                            SetBucket(newBucketPtr, slotPtr);

                            var nextSlot = GetSlot(slot.next);

                            SetSlot(slot.next, Slot.CreateSlot(nextSlot.hashCode, -1, nextSlot.next, nextSlot.keyPtr));
                            SetSlot(slotPtr, Slot.CreateSlot(hashCode, -1, -1, slot.keyPtr));
                        }
                        else if(slot.prev >= 0 && slot.next < 0)
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
                while (slotPtr>= 0);
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

            foreach(var fwdSlot in fwdSlots)
            {
                var keyPtr = fwdSlot.keyPtr;
                var key = GetKey(keyPtr);

                var value = GetValue(key.valuePtr);
            }
        }

        public void Dispose() {
            _pageManagerRunning = false;
            _pageManagerThread.Join();
            Flush();
        }

        public void Flush()
        {
            lock (_bucketCache)
            {
                while (_bucketPageIds.Count > 0)
                {
                    var pageId = _bucketPageIds.First.Value;
                    var page = _bucketCache[pageId];

                    if (page.Modified)
                        SavePage(_bucketStream, ref page);

                    _bucketCache.Remove(pageId);
                    _bucketPageIds.RemoveFirst();
                }
            }

            lock (_slotCache)
            {
                while (_slotPageIds.Count > 0)
                {
                    var pageId = _slotPageIds.First.Value;
                    var page = _slotCache[pageId];

                    if (page.Modified)
                        SavePage(_slotStream, ref page);

                    _slotCache.Remove(pageId);
                    _slotPageIds.RemoveFirst();
                }
            }

            lock (_keyCache)
            {
                while (_keyPageIds.Count > 0)
                {
                    var pageId = _keyPageIds.First.Value;
                    var page = _keyCache[pageId];

                    if (page.Modified)
                        SavePage(_keyStream, ref page);

                    _keyCache.Remove(pageId);
                    _keyPageIds.RemoveFirst();
                }
            }

            lock (_valueCache)
            {
                while (_valuePageIds.Count > 0)
                {
                    var pageId = _valuePageIds.First.Value;
                    var page = _valueCache[pageId];

                    if (page.Modified)
                        SavePage(_valueStream, ref page);

                    _valueCache.Remove(pageId);
                    _valuePageIds.RemoveFirst();
                }
            }
        }

        public IEnumerable<Key> GetKeys()
        {
            for(int i = 0; i < _capacity; i++)
            {
                var bucketPtr = i * _bucketSize;

                var slotPtr = GetBucket(bucketPtr);

                if (slotPtr == -1)
                    continue;

                var slots = EnumerateSlots(slotPtr);

                foreach(var slot in slots)
                {
                    var key = GetKey(slot.keyPtr);

                    yield return key.key;
                }
            }
        }

        public Value this[Key index]
        {
            get
            {
                var hashCode = (uint)index.GetHashCode();
                var bucketId = hashCode % _capacity;
                var indexPtr = bucketId * _bucketSize;

                var slotPtr = GetBucket(indexPtr);

                if (slotPtr == -1)
                    return default;

                do
                {
                    var slot = GetSlot(slotPtr);
                    var keyResult = GetKey(slot.keyPtr);

                    if (slot.hashCode == hashCode && keyResult.key.Equals(index))
                    {
                        return GetValue(keyResult.valuePtr);
                    }

                    slotPtr = slot.next;
                } while (slotPtr >= 0);

                return default;
            }

            set
            {
                var hashCode = (uint)index.GetHashCode();
                var bucketId = hashCode % _capacity;

                var indexPtr = bucketId * _bucketSize;

                var slotPtr = GetBucket(indexPtr);

                if (slotPtr == -1)
                {
                    var valuePtr = AddValue(value);
                    var keyPtr = AddKey(index, valuePtr);
                    slotPtr = AddSlot(Slot.CreateSlot(hashCode, -1, -1, keyPtr));
                    SetBucket(indexPtr, slotPtr);
                    _count++;
                }
                else
                {
                    long lastSlotPtr = GetLastSlotPtr(slotPtr);

                    var valuePtr = AddValue(value);
                    var keyPtr = AddKey(index, valuePtr);
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
