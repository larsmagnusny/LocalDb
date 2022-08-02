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
            Key<T> ret = new()
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
        public Page(long id, long from, long position, int itemsPerPage, T? defaultValue = default)
        {
            Id = id;
            From = from;
            Position = position + 8*id;
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
        public long Position { get; set; }
        public bool Modified { get; set; }
        public T[] Cache { get; set; }
        public DateTime TimeLoaded { get; set; }
        public int Hits { get; set; }

        public virtual void Save(Stream stream)
        {
            if (Modified)
            {
                stream.Seek(Position, SeekOrigin.Begin);
                stream.WriteUnmanaged(Id);
                for (long i = 0; i < Cache.Length; i++)
                {
                    stream.WriteManaged(Cache[i]);
                }
            }
        }

        public virtual bool LoadFromFile(Stream stream)
        {
            stream.Seek(Position, SeekOrigin.Begin);

            var id = stream.ReadUnmanaged<long>();

            if (Id != id)
                return false;

            for (long i = 0; i < Cache.Length; i++)
            {
                Cache[i] = stream.ReadManaged<T>();
            }

            Modified = false;

            return true;
        }
    }

    public class UnmanagedPage<T> : Page<T> where T : unmanaged
    {
        public UnmanagedPage(long id, long from, long position, int itemsPerPage, T defaultValue = default) : base(id, from, position, itemsPerPage, defaultValue)
        {

        }

        public override void Save(Stream stream)
        {
            if (Modified)
            {
                stream.Seek(Position, SeekOrigin.Begin);
                stream.WriteUnmanaged(Id);
                for (int i = 0; i < Cache.Length; i++)
                {
                    stream.WriteUnmanaged(Cache[i]);
                }
            }
        }

        public override bool LoadFromFile(Stream stream)
        {
            stream.Seek(Position, SeekOrigin.Begin);

            var id = stream.ReadUnmanaged<long>();

            if (Id != id)
                return false;

            for (int i = 0; i < Cache.Length; i++)
            {
                Cache[i] = stream.ReadUnmanaged<T>();
            }

            Modified = false;

            return true;
        }
    }

    public class PageCache<T>
    {
        public PageCache(string filePath, int cacheSizeBytes, bool isUnmanaged = false, T ? defaultValue = default)
        {
            Stream = File.Open(filePath, FileMode.OpenOrCreate);

            IsUnmanaged = isUnmanaged;
            DefaultValue = defaultValue;
            CacheSizeBytes = cacheSizeBytes;

            TotalPages = (int)(Stream.Length / CacheSizeBytes);
        }

        public FileStream Stream { get; set; }
        private Dictionary<long, Page<T>> Cache { get; set; } = new();
        private PriorityQueue<long, long> PageIds { get; set; } = new();
        public HashSet<long> SavedPages = new();

        public int TotalPages { get; set; }
        public int MaxPagesInMemory { get; set; }
        public int CacheSizeBytes { get; set; }
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
                int numToRemove = (PageIds.Count - MaxPagesInMemory + 1) + PageIds.Count / 5;

                lock (Stream)
                {
                    var count = 0;
                    while (count++ < numToRemove && PageIds.Count > 0)
                    {
                        var pageId = PageIds.Dequeue();
                        var page = Cache[pageId];

                        page.Save(Stream);

                        Cache.Remove(pageId);
                    }

                    //Console.WriteLine($"Removed {count - 1} pages");
                }
            }

            //GC.Collect();
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

        public Page<T> CreatePage(long id, long from, long position)
        {

            Page<T> ret;
            if (!IsUnmanaged)
            {
                return new Page<T>(id, from, position, ItemsPerPage, DefaultValue);
            }

            var t = typeof(UnmanagedPage<>);

            var tt = t.MakeGenericType(typeof(T));

            return (Page<T>)Activator.CreateInstance(tt, id, from, position, ItemsPerPage, DefaultValue);
        }

        public Page<T> LoadPage(long pageId)
        {
            lock (Cache)
            {
                // first
                long from = pageId * ItemsPerPage;

                Page<T> p;

                p = CreatePage(pageId, from, pageId * CacheSizeBytes);

                if (pageId < TotalPages)
                {
                    if (!p.LoadFromFile(Stream))
                        TotalPages++;
                }
                else
                    TotalPages++;

                Cache[pageId] = p;
                PageIds.Enqueue(pageId, pageId);

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
        private readonly PageCache<long> _bucketCache;
        private readonly PageCache<Slot> _slotCache;
        private readonly PageCache<Key<Key>> _keyCache;
        private readonly PageCache<Value> _valueCache;

        private const int _bucketSize = 8;
        private const int _slotSize = 32;
        private long _lastSlotPtr;

        private long _lastKeyPtr;

        private long _lastValuePtr;

        private long _capacity;
        private long _threshold => _capacity + 1;
        private long _count;


        private Type _keyType;
        private Type _valueType;

        //private Serializer<Key> _keySerializer;
        //private Serializer<Value> _valueSerializer;

        private Thread[] _pageManagerThreads;
        private bool _pageManagerRunning = true;

        public string CollectionName { get; private set; }

        public DiskIndex(string collectionName, bool cleanStart = false, long capacity = 127)
        {

            CollectionName = collectionName;
            if (cleanStart)
            {
                try
                {
                    File.Delete($"buckets_{CollectionName}.bin");
                    File.Delete($"slots_{CollectionName}.bin");
                    File.Delete($"keys_{CollectionName}.bin");
                    File.Delete($"values_{CollectionName}.bin");
                }
                catch { }
            }

            _keyType = typeof(Key);
            _valueType = typeof(Value);

            long MaxMemoryPerCache = 1 * 1024 * 1024 * 1024;

            int cacheSize = 4 * 1024;

            var bucketCacheSize = cacheSize;

            while (bucketCacheSize % _bucketSize != 0)
                bucketCacheSize++;

            _bucketCache = new PageCache<long>($"buckets_{CollectionName}.bin", cacheSize, true, -1)
            {
                ItemsPerPage = cacheSize / _bucketSize,
                MaxPagesInMemory = (int)(MaxMemoryPerCache / cacheSize)
            };

            var slotCacheSize = cacheSize; // _slotSize * x = 64 * 1024

            while (slotCacheSize % _slotSize != 0)
                slotCacheSize++;

            _slotCache = new PageCache<Slot>($"slots_{CollectionName}.bin", slotCacheSize, true)
            {
                ItemsPerPage = slotCacheSize / _slotSize,
                MaxPagesInMemory = (int)(MaxMemoryPerCache / slotCacheSize)
            };

            var keySize = SizeOfCache<Key>.SizeOf + 8;
            var keyCacheSize = cacheSize; // Multiple of keySize where KeySize * x = 64 * 1024

            while (keyCacheSize % keySize != 0)
                keyCacheSize++;


            _keyCache = new PageCache<Key<Key>>($"keys_{CollectionName}.bin", keyCacheSize, _keyType.IsUnmanaged(), Key<Key>.CreateKey(default, -1))
            {
                ItemsPerPage = keyCacheSize / keySize,
                MaxPagesInMemory = (int)(MaxMemoryPerCache / keyCacheSize)
            };

            var valueSize = SizeOfCache<Value>.SizeOf;
            var valueCacheSize = cacheSize;

            while (valueCacheSize % valueSize != 0)
                valueCacheSize++;


            _valueCache = new PageCache<Value>($"values_{CollectionName}.bin", valueCacheSize, _valueType.IsUnmanaged())
            {
                ItemsPerPage = valueCacheSize / valueSize,
                MaxPagesInMemory = (int)(MaxMemoryPerCache / valueCacheSize)
            };

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

            return p.Cache[valuePtr - p.From];
        }

        public long AddValue(Value v)
        {
            var valuePtr = _lastValuePtr;

            var pageId = _valueCache.GetPageId(valuePtr);

            var p = _valueCache.GetPage(pageId);

            p.Cache[valuePtr - p.From] = v;

            p.Modified = true;

            _lastValuePtr++;

            return valuePtr;
        }

        private Key<Key> GetKey(long keyPtr)
        {
            var pageId = _keyCache.GetPageId(keyPtr);

            Page<Key<Key>> p = _keyCache.GetPage(pageId);

            return p.Cache[keyPtr - p.From];
        }

        public long AddKey(Key<Key> key)
        {
            var keyPtr = _lastKeyPtr;

            var pageId = _keyCache.GetPageId(keyPtr);

            var p = _keyCache.GetPage(pageId);

            _lastKeyPtr++;

            p.Cache[keyPtr - p.From] = key;

            p.Modified = true;

            return keyPtr;
        }

        private void SetBucket(long bucketPtr, long slotPtr)
        {
            var pageId = _bucketCache.GetPageId(bucketPtr);

            var p = _bucketCache.GetPage(pageId);

            p.Modified = true;

            p.Cache[bucketPtr - p.From] = slotPtr;
        }

        private long GetBucket(long bucketPtr)
        {
            var pageId = _bucketCache.GetPageId(bucketPtr);

            var p = _bucketCache.GetPage(pageId);

            return p.Cache[bucketPtr - p.From];
        }

        private unsafe Slot* GetSlotPtr(long slotPtr)
        {
            var pageId = _slotCache.GetPageId(slotPtr);

            var p = _slotCache.GetPage(pageId);

            fixed(Slot* s = &p.Cache[slotPtr - p.From])
            {
                return s;
            }
        }

        private Slot GetSlot(long slotPtr)
        {
            var pageId = _slotCache.GetPageId(slotPtr);

            var p = _slotCache.GetPage(pageId);

            return p.Cache[slotPtr - p.From];
        }

        private void SetSlot(long slotPtr, Slot slot)
        {
            var pageId = _slotCache.GetPageId(slotPtr);

            var p = _slotCache.GetPage(pageId);

            p.Cache[slotPtr - p.From] = slot;
        }

        private long AddSlot(long hashCode, long prev, long next, long keyPtr)
        {
            var curSlotPtr = _lastSlotPtr;

            var pageId = _slotCache.GetPageId(curSlotPtr);

            var p = _slotCache.GetPage(pageId);

            Slot s = new Slot
            {
                HashCode = hashCode,
                Prev = prev,
                Next = next,
                KeyPtr = keyPtr
            };

            p.Cache[_lastSlotPtr - p.From] = s;

            if (next != -1)
            {
                var nextPageId = _slotCache.GetPageId(next);

                var nP = _slotCache.GetPage(nextPageId);

                nP.Cache[next - nP.From].Prev = curSlotPtr;

                nP.Modified = true;
            }

            if (prev != -1)
            {
                var prevPageId = _slotCache.GetPageId(prev);

                var pP = _slotCache.GetPage(prevPageId);

                pP.Cache[prev - pP.From].Next = curSlotPtr;

                pP.Modified = true;
            }

            p.Modified = true;

            _lastSlotPtr++;

            return curSlotPtr;
        }

        public class SlotResult
        {
            public long SlotPtr { get; set; }
            public Slot Slot;
        }

        private unsafe SlotResult GetLastSlot(long slotPtr)
        {
            HashSet<long> visited = new();
            Slot slot;
            long retPtr;

            do
            {
                retPtr = slotPtr;
                slot = GetSlot(slotPtr);

                if (!visited.Add(slotPtr))
                    Debugger.Break();

                slotPtr = slot.Next;
            } while (slotPtr >= 0);

            return new SlotResult { SlotPtr = retPtr, Slot = slot };
        }

        private unsafe SlotResult GetFirstSlot(long slotPtr)
        {
            HashSet<long> visited = new();

            Slot slot;
            long retPtr;
            
            do
            {
                retPtr = slotPtr;
                slot = GetSlot(slotPtr);

                if (!visited.Add(slotPtr))
                    Debugger.Break();

                slotPtr = slot.Prev;
            } while (slotPtr >= 0);

            return new SlotResult { SlotPtr = retPtr, Slot = slot };
        }

        public class SlotToAdd
        {
            public SlotToAdd(long slotPtr, long addToSlotPtr, Slot slot, Slot lastSlot)
            {
                SlotPtr = slotPtr;
                AddToSlotPtr = addToSlotPtr;
                Slot = slot;
                LastSlot = lastSlot;
            }

            public long SlotPtr { get; set; }
            public long AddToSlotPtr { get; set; }
            public Slot Slot;
            public Slot LastSlot;
        }

        public class SlotToChange
        {
            public SlotToChange(long slotPtr, long bucketPtr, Slot slot)
            {
                SlotPtr = slotPtr;
                BucketPtr = bucketPtr;
                Slot = slot;
            }

            public long SlotPtr { get; set; }
            public long BucketPtr { get; set; }
            public Slot Slot;
        }

        private unsafe void Recalculate(long oldBucketPtr, long newCapacity)
        {
            var slotPtr = GetBucket(oldBucketPtr);

            if (slotPtr == -1)
                return;           

            do
            {
                var slot = GetSlot(slotPtr);

                var newBucketPtr = ((uint)slot.HashCode % newCapacity);

                if (newBucketPtr == oldBucketPtr)
                {
                    if (slot.Next >= 0)
                    {
                        slotPtr = slot.Next;
                        continue;
                    }
                    return;
                }

                var newSlotPtr = GetBucket(newBucketPtr);

                if (newSlotPtr == -1)
                {
                    if (slot.Next >= 0 && slot.Prev == -1)
                    {
                        var nextSlotPtr = slot.Next;
                        SetBucket(oldBucketPtr, nextSlotPtr);
                        SetBucket(newBucketPtr, slotPtr);

                        var nextSlot = GetSlot(nextSlotPtr);
                        
                        nextSlot.Prev = -1;
                        SetSlot(nextSlotPtr, nextSlot);

                        slot.Next = -1;
                        slot.Prev = -1;
                        SetSlot(slotPtr, slot);

                        slotPtr = nextSlotPtr;
                        continue;
                    }
                    else if (slot.Prev == -1 && slot.Next == -1) // Just one item in bucket
                    {
                        SetBucket(newBucketPtr, slotPtr);
                        
                        slot.Next = -1;
                        slot.Prev = -1;
                        SetSlot(slotPtr, slot);

                        SetBucket(oldBucketPtr, -1);

                        break;
                    }
                    else if (slot.Prev >= 0 && slot.Next == -1) // When the previous slot did not get assigned a new bucket...
                    {
                        SetBucket(newBucketPtr, slotPtr);
                        var prevSlotPtr = slot.Prev;
                        var prevSlot = GetSlot(prevSlotPtr);

                        prevSlot.Next = -1;

                        SetSlot(prevSlotPtr, prevSlot);

                        slot.Next = -1;
                        slot.Prev = -1;
                        SetSlot(slotPtr, slot);

                        break;
                    }
                    else if (slot.Prev >= 0 && slot.Next >= 0)
                    {
                        SetBucket(newBucketPtr, slotPtr);
                        var prevSlotPtr = slot.Prev;
                        var prevSlot = GetSlot(prevSlotPtr);

                        var nextSlotPtr = slot.Next;
                        var nextSlot = GetSlot(nextSlotPtr);

                        prevSlot.Next = nextSlotPtr;

                        SetSlot(prevSlotPtr, prevSlot);

                        nextSlot.Prev = prevSlotPtr;

                        SetSlot(nextSlotPtr, nextSlot);

                        slot.Prev = -1;
                        slot.Next = -1;

                        SetSlot(slotPtr, slot);

                        slotPtr = nextSlotPtr;
                        continue;
                    }
                    else
                    {
                        Debugger.Break();
                    }
                }
                else
                {
                    var lastSlot = GetLastSlot(newSlotPtr);
                    
                    if (slot.Next >= 0 && slot.Prev == -1) // Has only next
                    {
                        var nextSlotPtr = slot.Next;
                        var nextSlot = GetSlot(nextSlotPtr);

                        nextSlot.Prev = -1;

                        SetSlot(nextSlotPtr, nextSlot);

                        lastSlot.Slot.Next = slotPtr;
                        SetSlot(lastSlot.SlotPtr, lastSlot.Slot);

                        slot.Prev = lastSlot.SlotPtr;
                        slot.Next = -1;

                        SetSlot(slotPtr, slot);

                        SetBucket(oldBucketPtr, nextSlotPtr);

                        slotPtr = nextSlotPtr;

                        continue;
                    }
                    else if (slot.Next == -1 && slot.Prev == -1)
                    {
                        lastSlot.Slot.Next = slotPtr;

                        SetSlot(lastSlot.SlotPtr, lastSlot.Slot);

                        slot.Next = -1;
                        slot.Prev = lastSlot.SlotPtr;

                        SetSlot(slotPtr, slot);

                        SetBucket(oldBucketPtr, -1);

                        break;
                    }
                    else if(slot.Prev >= 0 && slot.Next == -1) // When the previous slot did not get assigned a new bucket...
                    {
                        var prevSlotPtr = slot.Prev;
                        var prevSlot = GetSlot(prevSlotPtr);

                        prevSlot.Next = -1;

                        SetSlot(prevSlotPtr, prevSlot);

                        lastSlot.Slot.Next = slotPtr;
                        SetSlot(lastSlot.SlotPtr, lastSlot.Slot);

                        slot.Next = -1;
                        slot.Prev = lastSlot.SlotPtr;
                        SetSlot(slotPtr, slot);

                        break;
                    }
                    else if(slot.Prev >= 0 && slot.Next >= 0)
                    {
                        var prevSlotPtr = slot.Prev;
                        var prevSlot = GetSlot(prevSlotPtr);

                        var nextSlotPtr = slot.Next;
                        var nextSlot = GetSlot(nextSlotPtr);

                        prevSlot.Next = nextSlotPtr;

                        SetSlot(prevSlotPtr, prevSlot);

                        nextSlot.Prev = prevSlotPtr;

                        SetSlot(nextSlotPtr, nextSlot);

                        lastSlot.Slot.Next = slotPtr;
                        SetSlot(lastSlot.SlotPtr, lastSlot.Slot);

                        slot.Next = -1;
                        slot.Prev = lastSlot.SlotPtr;

                        SetSlot(slotPtr, slot);

                        slotPtr = nextSlotPtr;
                        continue;
                    }
                    else
                    {
                        Debugger.Break();
                    }
                }
            } while (slotPtr >= 0);
        }

        private void Grow(long newCapacity)
        {
            var oldCapacity = _capacity;
            _capacity = newCapacity;

            var sw = Stopwatch.StartNew();

            Console.Write($"Growing from {oldCapacity} to {newCapacity}");

            for (var i = 0; i <= oldCapacity; i++)
            {
                Recalculate(i, newCapacity);
            }

            Console.WriteLine($" - {sw.Elapsed}");
        }

        public class EnumeratedSlots
        {
            public long SlotPtr { get; set; }
            public long BucketPtr { get; set; }
            public Slot Slot;
        }

        public unsafe bool ValidateSlot(long slotPtr)
        {
            var ptr = slotPtr;
            HashSet<long> visited = new();
            Slot s;
            do
            {
                s = GetSlot(ptr);

                var k = GetKey(s.KeyPtr);

                var v = GetValue(k.ValuePtr);

                if (s.Next > _lastSlotPtr)
                    Debugger.Break();
                if (s.Prev > _lastSlotPtr)
                    Debugger.Break();

                if (!visited.Add(ptr))
                    return false;
                ptr = s.Next;
            } while (ptr >= 0);

            visited.Clear();
            ptr = slotPtr;

            do
            {
                s = GetSlot(ptr);

                var k = GetKey(s.KeyPtr);

                var v = GetValue(k.ValuePtr);

                if (s.Next > _lastSlotPtr)
                    Debugger.Break();
                if (s.Prev > _lastSlotPtr)
                    Debugger.Break();

                if (!visited.Add(ptr))
                    return false;
                ptr = s.Prev;
            } while (ptr >= 0);

            return true;
        }

        public void Dispose()
        {
            _pageManagerRunning = false;

            //for (int i = 0; i < _pageManagerThreads.Length; i++)
            //{
            //    _pageManagerThreads[i].Join();
            //}
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
            for (int i = 0; i < _keyCache.TotalPages; i++)
            {
                var p = _keyCache.GetPage(i);
                
                foreach(var key in p.Cache)
                {
                    if (key.ValuePtr != (long)-1)
                    {
                        yield return key.KeyValue;
                    }
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

                var iterations = 0;

                Slot slot;
                do
                {
                    slot = GetSlot(slotPtr);

                    var keyResult = GetKey(slot.KeyPtr);

                    if (slot.HashCode == hashCode && keyResult.KeyValue.Equals(index))
                    {
                        return GetValue(keyResult.ValuePtr);
                    }
                    slotPtr = slot.Next;

                    iterations++;
                } while (slot.Next >= 0);

                Console.WriteLine($"Failed to find value after {iterations} iterations");

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
                    var iterations = 0;

                    Slot slot;
                    do
                    {
                        slot = GetSlot(slotPtr);

                        var next = slot.Next;

                        if(next >= 0)
                            slotPtr = next;

                        var keyResult = GetKey(slot.KeyPtr);

                        if (slot.HashCode == hashCode && keyResult.KeyValue.Equals(index))
                        {
                            return;
                        }

                        iterations++;
                    } while (slot.Next >= 0);

                    var valuePtr = AddValue(value);
                    var keyPtr = AddKey(Key<Key>.CreateKey(index, valuePtr));
                    AddSlot(hashCode, slotPtr, -1, keyPtr);
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