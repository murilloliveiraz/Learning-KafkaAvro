using Avro;
using Avro.Specific;
using System;

namespace KafkaAvroTest.Models
{
    public partial class User : ISpecificRecord
    {
        public static readonly Schema _SCHEMA = Schema.Parse(@"
        {
          ""type"": ""record"",
          ""name"": ""User"",
          ""namespace"": ""KafkaAvroTest"",
          ""fields"": [
            {""name"": ""UserId"", ""type"": ""int""},
            {""name"": ""Name"", ""type"": ""string""}
          ]
        }");

        public Schema Schema { get { return _SCHEMA; } }

        public int UserId { get; set; }
        public string Name { get; set; } = string.Empty;

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0:
                    return UserId;
                case 1:
                    return Name;
                default:
                    throw new AvroRuntimeException("Bad index " + fieldPos + " in Avro record");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    UserId = (int)fieldValue;
                    break;
                case 1:
                    Name = (string)fieldValue;
                    break;
                default:
                    throw new AvroRuntimeException("Bad index " + fieldPos + " in Avro record");
            }
        }
    }
}