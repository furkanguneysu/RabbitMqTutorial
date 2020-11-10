﻿using System;
using RabbitMQ.Client;
using System.Text;

namespace NewTask
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", UserName = "admin", Password = "123456" };
            using(var connection = factory.CreateConnection())
            using(var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "task_queue",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null
                                    );
                var message = GetMessage(args);
                var body = Encoding.UTF8.GetBytes((string)message);
                
                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                channel.BasicPublish(exchange: "", routingKey: "task_queue", basicProperties: properties, body: body);
                Console.WriteLine("[x] Sent {0}", message);
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }

        private static object GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World");
        }
    }
}