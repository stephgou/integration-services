using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Runtime.InteropServices;
using Producer.Helpers;

//https://github.com/confluentinc/confluent-kafka-dotnet/issues/572
//librdkafka dependency for ssl were not installed on the host.
//Specifically the following files for 0.11.6 version of the library.
//https://gitter.im/edenhill/librdkafka?at=577444d18441a8124d8e2138


namespace Producer.EventHubsKafka
{
    public static class Functions
    {
        [DllImport("kernel32", SetLastError = true)]
        private static extern IntPtr LoadLibrary(string lpFileName);

        [FunctionName(nameof(PostToEventHubKafka))]
        public static async Task<HttpResponseMessage> PostToEventHubKafka(
            [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestMessage request,
            [OrchestrationClient]DurableOrchestrationClient client,
            ILogger log)
        {

            var is64 = IntPtr.Size == 8;
            try
            {
                var baseUri = new Uri(Assembly.GetExecutingAssembly().GetName().CodeBase);
                log.LogInformation(baseUri.LocalPath);

                var baseDirectory = Path.GetDirectoryName(baseUri.LocalPath);
                log.LogInformation(baseDirectory);

                var parentDirectory = Helper.Left(baseDirectory, baseDirectory.Length - 3);
                var resolvingDll = Path.Combine(parentDirectory, is64 ? "librdkafka\\x64\\librdkafka.dll" : "librdkafka\\x86\\librdkafka.dll");
                log.LogInformation(resolvingDll);
                LoadLibrary(resolvingDll);

                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\zlib.dll" : "librdkafka\\x86\\zlib.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\librdkafkacpp.dll" : "librdkafka\\x86\\librdkafkacpp.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\libzstd.dll" : "librdkafka\\x86\\libzstd.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\msvcp120.dll" : "librdkafka\\x86\\msvcp120.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\msvcr120.dll" : "librdkafka\\x86\\msvcr120.dll"));
            }
            catch (Exception) { }


            var inputObject = JObject.Parse(await request.Content.ReadAsStringAsync());
            var numberOfMessagesPerPartition = inputObject.Value<int>(@"NumberOfMessagesPerPartition");
            var numberOfPartitions = Convert.ToInt32(Environment.GetEnvironmentVariable("EventHubKafkaPartitions"));

            var workTime = -1;
            if (inputObject.TryGetValue(@"WorkTime", out var workTimeVal))
            {
                workTime = workTimeVal.Value<int>();
            }

            var orchestrationIds = new List<string>();
            var testRunId = Guid.NewGuid().ToString();
            for (var c = 1; c <= numberOfPartitions; c++)
            {
                var orchId = await client.StartNewAsync(nameof(GenerateMessagesForEventHubKafka),
                    new MessagesCreateRequest
                    {
                        TestRunId = testRunId,
                        NumberOfMessagesPerPartition = numberOfMessagesPerPartition,
                        ConsumerWorkTime = workTime,
                    });

                log.LogTrace($@"Kicked off message creation for session {c}...");

                orchestrationIds.Add(orchId);
            }

            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(request, orchestrationIds.First(), TimeSpan.FromMinutes(2));
        }

        [FunctionName(nameof(GenerateMessagesForEventHubKafka))]
        public static async Task<JObject> GenerateMessagesForEventHubKafka(
            [OrchestrationTrigger]DurableOrchestrationContext ctx,
            ILogger log)
        {
            var req = ctx.GetInput<MessagesCreateRequest>();

            var messages = Enumerable.Range(1, req.NumberOfMessagesPerPartition)
                    .Select(m =>
                    {
                        var enqueueTime = DateTime.UtcNow;
                        return new MessagesSendRequest
                        {
                            MessageId = m,
                            EnqueueTimeUtc = enqueueTime,
                            TestRunId = req.TestRunId,
                            ConsumerWorkTime = req.ConsumerWorkTime,
                        };
                    }).ToList();

            try
            {
                return await ctx.CallActivityAsync<bool>(nameof(PostMessagesToEventHubKafka), messages)
                    ? JObject.FromObject(new { req.TestRunId })
                    : JObject.FromObject(new { Error = $@"An error occurred executing orchestration {ctx.InstanceId}" });
            }
            catch (Exception ex)
            {
                log.LogError(ex, @"An error occurred queuing message generation to Event Hub");
                return JObject.FromObject(new { Error = $@"An error occurred executing orchestration {ctx.InstanceId}: {ex.ToString()}" });
            }
        }

        private const int MAX_RETRY_ATTEMPTS = 10;
        private static readonly Lazy<string> _messageContent = new Lazy<string>(() =>
        {
            using (var sr = new StreamReader(Assembly.GetExecutingAssembly().GetManifestResourceStream($@"Producer.messagecontent.txt")))
            {
                return sr.ReadToEnd();
            }
        });

        [FunctionName(nameof(PostMessagesToEventHubKafka))]
        public static async Task<bool> PostMessagesToEventHubKafka([ActivityTrigger]DurableActivityContext ctx,
            ILogger log)
        {
            string brokerList = Environment.GetEnvironmentVariable("EventHubKafkaFQDN");
            string connectionString = Environment.GetEnvironmentVariable("EventHubKafkaConnection");
            string topic = Environment.GetEnvironmentVariable("EventHubKafkaName");

            var config = new ProducerConfig
            {
                BootstrapServers = brokerList,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString
            };
#if DEBUG
            config.Debug = "security,broker,protocol";
#endif

            var messages = ctx.GetInput<IEnumerable<MessagesSendRequest>>();

            using (var producer = new ProducerBuilder<long, string>(config).SetKeySerializer(Serializers.Int64).SetValueSerializer(Serializers.Utf8).Build())
            {
                foreach (var messageToPost in messages.Select(m =>
                    {
                        var r = new MessagesSendRequest();
                        if (m.ConsumerWorkTime > 0)
                        {
                            r.ConsumerWorkTime = m.ConsumerWorkTime;
                        }
                        r.EnqueueTimeUtc = m.EnqueueTimeUtc;
                        r.MessageId = m.MessageId;
                        r.TestRunId = m.TestRunId;
                        r.Message = _messageContent.Value;

                        return r;
                    }
                ))
                {
                    var retryCount = 0;
                    var retry = false;
                    do
                    {
                        retryCount++;
                        try
                        {
                            var msg = JsonConvert.SerializeObject(messageToPost);
                            var deliveryReport = await producer.ProduceAsync(topic, new Message<long, string> { Key = DateTime.UtcNow.Ticks, Value = msg });
                            retry = false;
                        }
                        catch (Exception ex)
                        {
                            log.LogError(ex, $@"Error posting message with TestRunID '{messageToPost.TestRunId}' and MessageID '{messageToPost.MessageId}'. Retrying...");
                            retry = true;
                        }

                        if (retry && retryCount >= MAX_RETRY_ATTEMPTS)
                        {
                            log.LogError($@"Unable to post message with TestRunID '{messageToPost.TestRunId}' and MessageID '{messageToPost.MessageId}' after {retryCount} attempt(s). Giving up.");
                            break;
                        }
                        else
                        {
#if DEBUG
                            log.LogTrace($@"Posted message {messageToPost.MessageId} (Size: {messageToPost.Message.Length} bytes) in {retryCount} attempt(s)");
#else
#endif
                            // extract from else endif - issue in compilation on Azure Web Site log.LogTrace($@"Posted message for with TestRunID '{messageToPost.Properties[@"TestRunId"]}' and MessageID '{messageToPost.Properties[@"MessageId"]}' in {retryCount} attempt(s)");
                        }
                    } while (retry);
                }
            }

            return true;
        }
    }
}
