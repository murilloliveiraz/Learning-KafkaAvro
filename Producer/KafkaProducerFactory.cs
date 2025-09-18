using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaAvroTest.Domain;
using KafkaAvroTest.Domain.Interfaces;
using System.Diagnostics;
using System.Text;

namespace KafkaAvroTest;

public class KafkaProducerFactory : IMessageProducer
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly CachedSchemaRegistryClient _schemaRegistry;
    private readonly string _topic;

    public KafkaProducerFactory(KafkaConfig config)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config.BootstrapServers,
            MaxInFlight = 1, // Ordem garantida (mude para 5 para mais throughput)
            EnableIdempotence = true
        };
        _producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();

        var registryConfig = new SchemaRegistryConfig { Url = config.SchemaRegistryUrl };
        _schemaRegistry = new CachedSchemaRegistryClient(registryConfig);

        _topic = config.Topic;
    }

    public async Task ProduceAsync<T>(T message, string correlationId) where T : ISpecificRecord
    {
        var stopwatch = Stopwatch.StartNew();
        var serializerConfig = new AvroSerializerConfig
        {
            SubjectNameStrategy = SubjectNameStrategy.Record
        };
        var serializer = new AvroSerializer<T>(_schemaRegistry, serializerConfig);
        // Removido subjectName, usa apenas tópico com RecordName
        var context = new SerializationContext(MessageComponentType.Value, _topic);
        var data = await serializer.SerializeAsync(message, context);

        var kafkaMessage = new Message<Null, byte[]>
        {
            Value = data,
            Headers = new Headers()
        };
        kafkaMessage.Headers.Add("CorrelationId", Encoding.UTF8.GetBytes(correlationId));
        kafkaMessage.Headers.Add("MessageType", Encoding.UTF8.GetBytes(typeof(T).Name));

        var deliveryResult = await _producer.ProduceAsync(_topic, kafkaMessage);
        stopwatch.Stop();
        Console.WriteLine($"Produzido: {typeof(T).Name}, CorrelationId: {correlationId}, Offset: {deliveryResult.Offset}, Tempo: {stopwatch.ElapsedMilliseconds}ms");
    }
}