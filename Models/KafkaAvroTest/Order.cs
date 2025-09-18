using Avro;
using Avro.Specific;
using System;

namespace KafkaAvroTest.Models
{
    public partial class Order : ISpecificRecord
    {
        public static readonly Schema _SCHEMA = Schema.Parse(@"
        {
          ""type"": ""record"",
          ""name"": ""Order"",
          ""namespace"": ""KafkaAvroTest"",
          ""fields"": [
            {""name"": ""Id"", ""type"": ""int""},
            {""name"": ""Product"", ""type"": ""string""},
            {""name"": ""Amount"", ""type"": [""null"", ""double""], ""default"": null}
          ]
        }");

        public Schema Schema { get { return _SCHEMA; } }

        public int Id { get; set; }
        public string Product { get; set; } = string.Empty;
        public double? Amount { get; set; }

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0:
                    return Id;
                case 1:
                    return Product;
                case 2:
                    return Amount;
                default:
                    throw new AvroRuntimeException("Bad index " + fieldPos + " in Avro record");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    Id = (int)fieldValue;
                    break;
                case 1:
                    Product = (string)fieldValue;
                    break;
                case 2:
                    Amount = (double?)fieldValue;
                    break;
                default:
                    throw new AvroRuntimeException("Bad index " + fieldPos + " in Avro record");
            }
        }
    }
}