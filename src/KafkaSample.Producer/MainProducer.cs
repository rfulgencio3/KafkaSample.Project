using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaSample.Producer
{
    public class MainProducer : BackgroundService
    {
        private readonly IConfigurationRoot _configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", optional: true)
            .Build();
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await ProducerProcessor();
        }
        public async Task ProducerProcessor()
        {
            var bootstrapServers = _configuration["KAFKA_BOOTSTRAP_SERVERS"];
            var topic = _configuration["KAFKA_BOOTSTRAP_TOPIC"];
            ProducerConfig config = new ProducerConfig();

            if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Local")
            {
                config = new ProducerConfig { BootstrapServers = bootstrapServers };
            }
            else
            {
                config = new ProducerConfig 
                { 
                    BootstrapServers = bootstrapServers,
                    SaslUsername = _configuration["CLOUDKARAFKA_USERNAME"],
                    SaslPassword = _configuration["CLOUDKARAFKA_PASSWORD"],
                    SaslMechanism = SaslMechanism.ScramSha256,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    EnableSslCertificateVerification = false
                };
            }

            using (var builder = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var count = 0;
                    while (count <= 100)
                    {
                        var producer = await builder.ProduceAsync(topic,
                            new Message<Null, string> { Value = $"publish-test: {count}" });

                        Console.WriteLine($"Delivered '{producer.Value}' to '{producer.TopicPartitionOffset}'");
                        count++;

                        Thread.Sleep(2000);
                    }
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
                }
            }
        }


    }
}
