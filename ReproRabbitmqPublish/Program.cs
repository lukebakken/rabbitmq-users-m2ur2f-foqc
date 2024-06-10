using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReproRabbitmqPublish
{
	public class Program
	{
		struct MessageReplayData
		{
			public int MillisecondDelay;
			public string Exchange;
			public int MessageSize;
			public int BatchCount;
		}

		static List<MessageReplayData> ReplayData = new List<MessageReplayData> {
			new MessageReplayData { MillisecondDelay = 0, Exchange = "1", MessageSize = 903, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 0, Exchange = "2", MessageSize = 670, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 0, Exchange = "4", MessageSize = 4024, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 2, Exchange = "5", MessageSize = 121, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 4, Exchange = "3", MessageSize = 788, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 4, Exchange = "6", MessageSize = 40456, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 5, Exchange = "2", MessageSize = 324, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 5, Exchange = "1", MessageSize = 324, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 8, Exchange = "6", MessageSize = 324, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 8, Exchange = "3", MessageSize = 371, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 9, Exchange = "5", MessageSize = 411, BatchCount = 1 },
			new MessageReplayData { MillisecondDelay = 10, Exchange = "4", MessageSize = 464, BatchCount = 1 },
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

		const int DuplicateFactor = 5;

		public static async Task Main(string[] args)
		{
			// Test was run with latest RabbitMQ broker on a windows desktop, RabbitMQ 3.11.8 Erlang 25.2.2
			var factory = new ConnectionFactory() {
				Uri = new Uri($"amqp://guest:guest@localhost:5672/"),
				ContinuationTimeout = TimeSpan.FromSeconds(1), // this defaults to 20 seconds, set to 1 just to fail faster here
			};

			ThreadPool.GetMaxThreads(out var maxThreads, out var maxPorts);
			ThreadPool.GetMinThreads(out var minThreads, out var minPorts);
			Console.WriteLine($"Min {minThreads:N0} {minPorts:N0} Max {maxThreads:N0} {maxPorts:N0}");

			using (IConnection connection = await factory.CreateConnectionAsync("AppServ"))
			{
				// Min threads must be MORE than number of simultaneous messages being sent (+ small overhead) or BlockingCell.WaitForValue will timeout
				// ThreadPool.SetMinThreads(ReplayData.Count * DuplicateFactor + 10, minPorts);
				// start sending messages
				var tasks = new List<Task>();
				foreach (MessageReplayData message in ReplayData)
				{
					for (var duplicate = 0; duplicate < DuplicateFactor; duplicate++)
					{
						tasks.Add(Task.Run(() => Send(message, connection)));
					}
				}

				Console.WriteLine("[INFO] waiting for all messages to send...");
				await Task.WhenAll(tasks);
				Console.WriteLine("[INFO] done, sent all messages.");
			}
		}

		private static async Task Send(MessageReplayData message, IConnection connection)
		{
			await Task.Delay(TimeSpan.FromMilliseconds(message.MillisecondDelay));
			try
			{
				var sending = Interlocked.Increment(ref Sending);
				Console.WriteLine($"Sending: {sending}");

				var msg = new byte[message.MessageSize];
				var r = new Random();
				r.NextBytes(msg);

				using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
				{
					using (IChannel channel = await connection.CreateChannelAsync(cancellationToken: cts.Token))
					{
						await channel.ExchangeDeclareAsync(message.Exchange, "headers", durable: true, autoDelete: false, arguments: null,
							cancellationToken: cts.Token);
						await channel.ConfirmSelectAsync(cancellationToken: cts.Token);

						int ackNackCount = 0;
						var allMessagesConfirmedTcs = new TaskCompletionSource<bool>();
						using (CancellationTokenRegistration ctr = cts.Token.Register(() => {
							allMessagesConfirmedTcs.TrySetCanceled();
						}))
						{
							channel.BasicAcks += (object sender, BasicAckEventArgs ea) => {
								Interlocked.Increment(ref ackNackCount);
								if (ackNackCount >= message.BatchCount)
								{
									allMessagesConfirmedTcs.SetResult(true);
								}
							};

							channel.BasicNacks += (object sender, BasicNackEventArgs ea) => {
								Interlocked.Increment(ref ackNackCount);
								if (ackNackCount >= message.BatchCount)
								{
									allMessagesConfirmedTcs.SetResult(true);
								}
							};

							var publishTasks = new List<Task>();
							for (var messageCount = 0; messageCount < message.BatchCount; messageCount++)
							{
								var properties = new BasicProperties();
								properties.Type = "Commit";
								properties.ContentType = "application/LZ4";
								properties.ContentEncoding = "LZ4";
								properties.AppId = "Server";
								properties.Headers = new Dictionary<string, object> { { "EventType", "Commit" }, { "UserName", "user" } };
								publishTasks.Add(channel.BasicPublishAsync(message.Exchange, string.Empty, properties, msg,
									mandatory: true, cancellationToken: cts.Token).AsTask());
							}

							await Task.WhenAll(publishTasks);
							await allMessagesConfirmedTcs.Task;
							await channel.CloseAsync();
						}
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
