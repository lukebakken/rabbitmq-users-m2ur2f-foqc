using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace ReproRabbitmqPublish
{
	internal class Program
	{
		struct MessageReplayData
		{
			public int MillisecondDelay;
			public string Exchange;
			public int MessageSize;
			public int BatchCount;
		}

		static List<MessageReplayData> ReplayData = new List<MessageReplayData> {
			new MessageReplayData { MillisecondDelay = 0, Exchange = "1", MessageSize = 903 },
			new MessageReplayData { MillisecondDelay = 0, Exchange = "2", MessageSize = 670 },
			new MessageReplayData { MillisecondDelay = 0, Exchange = "4", MessageSize = 4024 },
			new MessageReplayData { MillisecondDelay = 2, Exchange = "5", MessageSize = 121 },
			new MessageReplayData { MillisecondDelay = 4, Exchange = "3", MessageSize = 788 },
			new MessageReplayData { MillisecondDelay = 4, Exchange = "6", MessageSize = 40456 },
			new MessageReplayData { MillisecondDelay = 5, Exchange = "2", MessageSize = 324 },
			new MessageReplayData { MillisecondDelay = 5, Exchange = "1", MessageSize = 324 },
			new MessageReplayData { MillisecondDelay = 8, Exchange = "6", MessageSize = 324 },
			new MessageReplayData { MillisecondDelay = 8, Exchange = "3", MessageSize = 371 },
			new MessageReplayData { MillisecondDelay = 9, Exchange = "5", MessageSize = 411 },
			new MessageReplayData { MillisecondDelay = 10, Exchange = "4", MessageSize = 464 },

			new MessageReplayData { MillisecondDelay = 56, Exchange = "4", MessageSize = 2000, BatchCount = 2 },
			new MessageReplayData { MillisecondDelay = 56, Exchange = "4", MessageSize = 2000, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 56, Exchange = "4", MessageSize = 2000, BatchCount = 14 },
			new MessageReplayData { MillisecondDelay = 56, Exchange = "4", MessageSize = 2000, BatchCount = 14 },
			new MessageReplayData { MillisecondDelay = 56, Exchange = "4", MessageSize = 2000, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 66, Exchange = "4", MessageSize = 2000, BatchCount = 5 },
			new MessageReplayData { MillisecondDelay = 66, Exchange = "4", MessageSize = 2000, BatchCount = 8 },
		};

		static int Sending = 0;
		static int Sent = 0;

		static void Main(string[] args)
		{
			// Test was run with latest RabbitMQ broker on a windows desktop, RabbitMQ 3.11.8 Erlang 25.2.2
			var factory = new ConnectionFactory() {
				Uri = new Uri($"amqp://guest:guest@localhost:5672/"),

				ContinuationTimeout = TimeSpan.FromSeconds(1), // this defaults to 20 seconds, set to 1 just to fail faster here
			};
			var connection = factory.CreateConnection("AppServ");

			ThreadPool.GetMaxThreads(out var maxThreads, out var maxPorts);
			ThreadPool.GetMinThreads(out var minThreads, out var minPorts);
			Console.WriteLine($"Min {minThreads:N0} {minPorts:N0} Max {maxThreads:N0} {maxPorts:N0}");

			// Setting min threads prevents timeouts
			//ThreadPool.SetMinThreads(16 * Environment.ProcessorCount, 16 * Environment.ProcessorCount);

			// start sending messages
			foreach (var message in ReplayData)
			{
				for (var duplicate = 0; duplicate < 5; duplicate++)
				{
					Task.Factory.StartNew(() => WaitAndDispatch(message, connection));
				}
			}

			// Only exit if all were sent
			while (Sent < (ReplayData.Count * 5))
			{
				Thread.Sleep(1);
			}

			// Let the other WriteLines finish first
			Thread.Sleep(100);

			Console.WriteLine();
			Console.WriteLine("Done");
			Console.ReadLine();
		}

		static void WaitAndDispatch(MessageReplayData message, IConnection connection)
		{
			if (message.MillisecondDelay > 0)
			{
				Task.Delay(TimeSpan.FromMilliseconds(message.MillisecondDelay));
			}

			Task.Factory.StartNew(() => Send(message, connection));
		}

		private static void Send(MessageReplayData message, IConnection connection)
		{
			try
			{
				var sending = Interlocked.Increment(ref Sending);
				Console.WriteLine($"Sending: {sending}");

				if (message.BatchCount != 0)
				{
					using (var channel = connection.CreateModel())
					{
						channel.ExchangeDeclare(message.Exchange, "headers", durable: true, autoDelete: false, arguments: null);
						channel.ConfirmSelect();

						var batch = channel.CreateBasicPublishBatch();

						for (var messageCount = 0; messageCount < message.BatchCount; messageCount++)
						{
							var properties = channel.CreateBasicProperties();
							properties.Type = "Commit";
							properties.ContentType = "application/LZ4";
							properties.ContentEncoding = "LZ4";
							properties.AppId = "Server";
							properties.Headers = new Dictionary<string, object> { { "EventType", "Commit" }, { "UserName", "user" } };

							batch.Add(message.Exchange, string.Empty, false, properties, new byte[message.MessageSize]);
						}

						batch.Publish();
						channel.WaitForConfirmsOrDie(TimeSpan.FromMinutes(1));
					}
				}
				else
				{
					using (var channel = connection.CreateModel())
					{
						channel.ExchangeDeclare(message.Exchange, "headers", durable: true, autoDelete: false, arguments: null);

						var properties = channel.CreateBasicProperties();
						properties.Type = "Query";
						properties.ContentType = "application/LZ4";
						properties.ContentEncoding = "LZ4";
						properties.AppId = "Server";
						properties.Headers = new Dictionary<string, object> { { "EventType", "Query" }, { "UserName", "user" } };

						channel.BasicPublish(message.Exchange, string.Empty, false, properties, new byte[message.MessageSize]);
					}
				}

				Interlocked.Decrement(ref Sending);

				var sentCount = Interlocked.Increment(ref Sent);
				Console.WriteLine($"Sent {sentCount}");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Send failed {ex}");
			}
		}
	}
}
