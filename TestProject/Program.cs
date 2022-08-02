// See https://aka.ms/new-console-template for more information
using Bogus;
using DB;
using System.Diagnostics;
using System.Text;
using TestProject;

var num = 100000000;

var d = new DiskIndex<int, int>("guidCollection", true);

//var generator = new Faker<TestEntity>()
//    .RuleFor(o => o.Id, f => f.Random.Int())
//    .RuleFor(o => o.Ticks, f => f.Random.Long())
//    .RuleFor(o => o.Balance, f => f.Random.Float())
//    .RuleFor(o => o.Name, f => $"{f.Name.FirstName()} {f.Name.LastName()}");

var random = new Random(3499);

var sw = Stopwatch.StartNew();

var counter = 0;
for(int i = 0; i < num; i++)
{
    var id = random.Next();
    d[id] = i;

    if (counter++ % 10000 == 0)
        Console.WriteLine($"id: {id} val: {i} - {sw.ElapsedMilliseconds * 1000000.0 / (double)counter} ns");
}

d.Flush();

Console.WriteLine("Viewing result:");

sw.Restart();

counter = 0;
foreach(var id in d.GetKeys())
{
    var val = d[id];

    if(counter++ % 10000 == 0)
        Console.WriteLine($"id: {id} val: {val} - {sw.ElapsedMilliseconds * 1000000.0 / (double)counter} ns");
}

d.Dispose();