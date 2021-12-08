﻿using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaSample.Consumer
{
    public class MainConsumer : BackgroundService
    {
        private readonly IConfigurationRoot _configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", optional: true)
            .Build();
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await ConsumerProcessor();
        }
        public async Task ConsumerProcessor()
        {
            var bootstrapServers = _configuration["KAFKA_BOOTSTRAP_SERVERS"];
            var topic = _configuration["KAFKA_BOOTSTRAP_TOPIC"];
            ConsumerConfig config = new ConsumerConfig();

            if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Local")
            {
                config = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = "test-consumer-group",
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };
            }
            else
            {
                config = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = "test-consumer-group",
                    SaslUsername = _configuration["CLOUDKARAFKA_USERNAME"],
                    SaslPassword = _configuration["CLOUDKARAFKA_PASSWORD"],
                    SaslMechanism = SaslMechanism.ScramSha256,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    EnableSslCertificateVerification = false,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };
            }

            using (var builder = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                builder.Subscribe("test-publish-topic");

                var cancellationToken = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cancellationToken.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumer = builder.Consume(cancellationToken.Token);
                            Console.WriteLine($"Consumed message '{consumer.Value}' at: '{ consumer.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException ex)
                        {
                            Console.Write($"Error ocurred: {ex.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    builder.Close();
                }
            }
        }
    }
}