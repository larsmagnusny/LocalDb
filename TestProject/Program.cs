// See https://aka.ms/new-console-template for more information
using Bogus;
using DB;
using System.Diagnostics;
using System.Text;
using TestProject;

// Test write speed

FileStream testStream = File.Open("Test.bin", FileMode.OpenOrCreate);


using var memoryStream = new MemoryStream();
for(int i = 0; i < int.MaxValue; i++)
{
    testStream.WriteInt(i);
}

testStream.Seek(0, SeekOrigin.Begin);

for(int i = 0; i < int.MaxValue; i++)
{
    int t = testStream.ReadInt();
}

//memoryStream.CopyTo(testStream);

testStream.Flush();


return;
var num = 1000;

var d = new DiskIndex<long, long>(num);

var generator = new Faker<TestEntity>()
    .RuleFor(o => o.Id, f => f.Random.Int())
    .RuleFor(o => o.Ticks, f => f.Random.Long())
    .RuleFor(o => o.Balance, f => f.Random.Float())
    .RuleFor(o => o.Name, f => $"{f.Name.FirstName()} {f.Name.LastName()}");

for(long i = 0; i < num; i++)
{
    var id = i;
    d[id] = i;

    //if (i % 1000000 == 0)
        Console.WriteLine($"id: {id} val: {i}");
}

d.Flush();

Console.WriteLine("Viewing result:");

var counter = 0;
foreach(var key in d.GetKeys())
{
    var id = key;
    var val = d[id];

    //if(counter++ % 1000000 == 0)
        Console.WriteLine($"id: {id} val: {val}");
}

d.Dispose();