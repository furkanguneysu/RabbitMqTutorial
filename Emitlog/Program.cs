﻿using System;
using RabbitMQ.Client;
using System.Text;

namespace Emitlog
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", UserName="admin", Password="123456" };
            using(var connection = factory.CreateConnection())
            {
                using(var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes((string)message);
                    channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: body);
                    Console.WriteLine(" [x] Sent {0}", message);
                }

                Console.WriteLine("Press [enter]to exit.");
                Console.ReadLine();
            }
        }

        private static object GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "info: Hello World!");
        }
    }
}
