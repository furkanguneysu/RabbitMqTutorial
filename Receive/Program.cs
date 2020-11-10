using System;
using RabbitMQ;
using RabbitMQ.Client;
using System.Text;
using RabbitMQ.Client.Events;

namespace Receive
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost",UserName="admin",Password="123456" };
            using(var connection = factory.CreateConnection())
            {
                using(var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "hello",
                                         durable:false,
                                         exclusive:false,
                                         autoDelete:false,
                                         arguments:null
                                        );
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine("[x] Received {0}", message);
                    };
                    channel.BasicConsume(queue: "hello",
                                         autoAck:true,
                                         consumer:consumer
                                        );
                    Console.WriteLine("Press [enter] to exit");
                    Console.ReadLine();
                }
            }
        }
    }
}
