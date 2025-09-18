using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaAvroTest.Domain;
using KafkaAvroTest.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Text;

namespace KafkaAvroTest;

public class KafkaConsumerService : BackgroundService
{
    private readonly IConsumer<Null, byte[]> _consumer;
    private readonly CachedSchemaRegistryClient _schemaRegistry;
    private readonly string _topic;
    private readonly ILogger<KafkaConsumerService> _logger;

    public KafkaConsumerService(KafkaConfig config, ILogger<KafkaConsumerService> logger)
    {
        _logger = logger;
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config.BootstrapServers,
            GroupId = "event-consumers-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build();

        var registryConfig = new SchemaRegistryConfig { Url = config.SchemaRegistryUrl };
        _schemaRegistry = new CachedSchemaRegistryClient(registryConfig);

        _topic = config.Topic;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        _logger.LogInformation("Consumer iniciado no tópico {Topic}", _topic);

        Task.Run(async () =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var stopwatch = Stopwatch.StartNew();
                    var consumeResult = _consumer.Consume(stoppingToken);

                    var correlationId = consumeResult.Message.Headers?.FirstOrDefault(h => h.Key == "CorrelationId")?.GetValueBytes();
                    var correlationIdStr = correlationId != null ? Encoding.UTF8.GetString(correlationId) : "N/A";

                    var messageType = consumeResult.Message.Headers?.FirstOrDefault(h => h.Key == "MessageType")?.GetValueBytes();
                    var messageTypeStr = messageType != null ? Encoding.UTF8.GetString(messageType) : "Unknown";

                    object? message = null;
                    var context = new SerializationContext(MessageComponentType.Value, _topic);
                    if (messageTypeStr == typeof(Order).Name)
                    {
                        var deserializer = new AvroDeserializer<Order>(_schemaRegistry, new AvroDeserializerConfig
                        {
                            SubjectNameStrategy = SubjectNameStrategy.Record
                        });
                        message = await deserializer.DeserializeAsync(consumeResult.Message.Value, consumeResult.Message.Value != null, context);
                    }
                    else if (messageTypeStr == typeof(User).Name)
                    {
                        var deserializer = new AvroDeserializer<User>(_schemaRegistry, new AvroDeserializerConfig
                        {
                            SubjectNameStrategy = SubjectNameStrategy.Record
                        });
                        message = await deserializer.DeserializeAsync(consumeResult.Message.Value, consumeResult.Message.Value != null, context);
                    }

                    stopwatch.Stop();
                    switch (message)
                    {
                        case Order order:
                            _logger.LogInformation("Order consumido: CorrelationId={CorrelationId}, ID={Id}, Product={Product}, Amount={Amount}, Tempo={Tempo}ms",
                                correlationIdStr, order.Id, order.Product, order.Amount, stopwatch.ElapsedMilliseconds);
                            break;
                        case User user:
                            _logger.LogInformation("User consumido: CorrelationId={CorrelationId}, ID={Id}, Name={Name}, Tempo={Tempo}ms",
                                correlationIdStr, user.UserId, user.Name, stopwatch.ElapsedMilliseconds);
                            break;
                        default:
                            _logger.LogWarning("Tipo desconhecido: {Type}, CorrelationId={CorrelationId}", messageTypeStr, correlationIdStr);
                            break;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro ao consumir mensagem");
                }
            }
        }, stoppingToken);

        return Task.CompletedTask;
    }

    public override void Dispose()
    {
        _consumer.Close();
        _schemaRegistry.Dispose();
        base.Dispose();
    }
}