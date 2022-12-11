using RabbitMQ.Client;
using System.Text.Json;
using System.Text;
using RabbitMQ.Client.Events;
using ItemService.EventProcessing;

namespace ItemService.RabbitMqClient
{
    public class RabbitMqSubscriber : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly string _queueName;
        private IProcessaEvento _processEvent;

        public RabbitMqSubscriber(IConfiguration configuration, IProcessaEvento processEvent)
        {
            _configuration = configuration;

            _connection = new ConnectionFactory()
            {
                HostName = _configuration["RabbitMQHost"],
                Port = Int32.Parse(_configuration["RabbitMQPort"])
            }.CreateConnection();

            _channel = _connection.CreateModel();
            _channel.ExchangeDeclare(exchange: "trigger",
                                     type: ExchangeType.Fanout);
            _queueName = _channel.QueueDeclare().QueueName;
            _channel.QueueBind(queue: _queueName,
                               exchange: "trigger",
                               routingKey: "");
            _processEvent = processEvent;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new EventingBasicConsumer(_channel);

            consumer.Received += (moduleHandle, eventArgs) =>
            {
                var body = eventArgs.Body;
                var message = Encoding.UTF8.GetString(body.ToArray());
                _processEvent.Processa(message);
            };

            _channel.BasicConsume(queue: _queueName, autoAck: true, consumer);

            return Task.CompletedTask;
        }
    }
}
