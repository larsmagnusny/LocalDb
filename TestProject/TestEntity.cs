using System.ComponentModel.DataAnnotations;

namespace TestProject
{
    public class TestEntity
    {
        public int? Id { get; set; }
        public string Name { get; set; } = string.Empty;
        
        public long Timestamp { get; set; }
        
        public float Balance { get; set; }

        public int Age { get; set; }

        public long Ticks { get; set; }
    }
}
