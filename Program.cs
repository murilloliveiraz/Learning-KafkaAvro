using KafkaAvroTest;
using KafkaAvroTest.Domain;
using KafkaAvroTest.Domain.Interfaces;
using KafkaAvroTest.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton(new KafkaConfig
{
    BootstrapServers = "localhost:9092",
    SchemaRegistryUrl = "http://localhost:8081",
    Topic = "events-topic"
});

builder.Services.AddSingleton<IMessageProducer, KafkaProducerFactory>();
builder.Services.AddHostedService<KafkaConsumerService>();

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var producer = scope.ServiceProvider.GetRequiredService<IMessageProducer>();
    await producer.ProduceAsync(new Order { Id = 1, Product = "Laptop", Amount = 999.99 }, Guid.NewGuid().ToString());
    await producer.ProduceAsync(new User { UserId = 101, Name = "Alice" }, Guid.NewGuid().ToString());
    Console.WriteLine("Mensagens enviadas! Aguardando consumer...");
}

await host.RunAsync();
