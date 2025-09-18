namespace KafkaAvroTest.Domain
{
    public class KafkaConfig
    {
        public string BootstrapServers { get; set; } = string.Empty;
        public string SchemaRegistryUrl { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
    }
}
