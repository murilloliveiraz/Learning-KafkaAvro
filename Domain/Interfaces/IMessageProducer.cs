using Avro.Specific;

namespace KafkaAvroTest.Domain.Interfaces
{
    public interface IMessageProducer
    {
        Task ProduceAsync<T>(T message, string correlationId) where T : ISpecificRecord;
    }
}
