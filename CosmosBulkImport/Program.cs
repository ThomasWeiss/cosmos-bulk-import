using Akka.Actor;
using CommandLine;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CosmosBulkImport
{
    static class DateTimeExtensions
    {
        public static int SecondsSince(this DateTime dateTime)
        {
            return (int)(DateTime.Now - dateTime).TotalSeconds;
        }
    }

    public class BulkImporterActor : UntypedActor
    {
        public class StartMessage { }
        public class UploadRequestMessage { public Document Document { get; set; } }
        public class UploadFailedMessage { public string Content { get; set; } public bool IsThrottle { get; set; } }
        public class PrintStatsMessage { }

        class ThrottlingData { public int RequestCount { get; set; } public int ThrottleCount { get; set; } }

        public BulkImporterActor(Options options)
        {
            _options = options;
        }

        private Options _options;

        private int _dataSize = 0;
        private int _dataProcessed = 0;
        private int _inFlightRequests = 0;
        private Dictionary<string, Document> _inFlightsDocs = new Dictionary<string, Document>();
        private Dictionary<int, ThrottlingData> _throttlingStats = new Dictionary<int, ThrottlingData>();

        private DateTime _startTime;
        private DateTime _lastStatUpdate;
        private DateTime _lastScaleOut;
        private int _dataProcessedSinceLastUpdate = 0;
        private double _requestUnitsUsedSinceLastUpdate = 0;

        private StreamReader _fileReader;
        private DocumentClient _documentClient;
        private Uri _collectionUri;

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case StartMessage startMessage:
                    _fileReader = File.OpenText(_options.InputFile);
                    if (!int.TryParse(_fileReader.ReadLine(), out _dataSize))
                    {
                        Console.WriteLine("Could not read data size from file");
                    }

                    _documentClient = new DocumentClient(new Uri(_options.Endpoint), _options.Key, new ConnectionPolicy
                    {
                        ConnectionMode = ConnectionMode.Direct,
                        ConnectionProtocol = Protocol.Tcp,
                        MaxConnectionLimit = int.MaxValue,
                        RetryOptions = new RetryOptions
                        {
                            MaxRetryAttemptsOnThrottledRequests = 0
                        }
                    });
                    _collectionUri = UriFactory.CreateDocumentCollectionUri(_options.Database, _options.Collection);

                    _startTime = DateTime.Now;
                    _lastStatUpdate = DateTime.Now;
                    _lastScaleOut = DateTime.Now;
                    Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0), TimeSpan.FromSeconds(1), Self, new PrintStatsMessage(), ActorRefs.NoSender);

                    DispatchUploadRequests();
                    break;
                case UploadRequestMessage uploadRequestMessage:
                    _inFlightRequests++;
                    _inFlightsDocs.Add(uploadRequestMessage.Document.Id, uploadRequestMessage.Document);
                    _documentClient.UpsertDocumentAsync(_collectionUri, uploadRequestMessage.Document).PipeTo(Self, null, resourceResponse =>
                    {
                        _inFlightsDocs.Remove(resourceResponse.Resource.Id);
                        return resourceResponse;
                    });
                    break;
                case ResourceResponse<Document> resourceResponse:
                    _inFlightRequests--;
                    UpdateStats(resourceResponse);
                    DispatchUploadRequests();
                    //if (!DispatchUploadRequests())
                    //{
                    //    Console.WriteLine();
                    //    Console.WriteLine("done");
                    //    Context.Stop(Self);
                    //}
                    break;
                case Status.Failure failure:
                    _inFlightRequests--;
                    //_failedUploads.Enqueue()
                    if (failure.Cause is AggregateException aggEx)
                    {
                        if ((aggEx.InnerException is DocumentClientException ex) && ((int)ex.StatusCode == 429))
                        {
                            var secondsSinceStart = _startTime.SecondsSince();
                            if (!_throttlingStats.ContainsKey(secondsSinceStart))
                            {
                                _throttlingStats.Add(secondsSinceStart, new ThrottlingData());
                            }
                            _throttlingStats[secondsSinceStart].ThrottleCount++;
                        }
                    }
                    else
                    {
                        if ((failure.Cause is DocumentClientException ex) && ((int)ex.StatusCode == 429))
                        {
                            var secondsSinceStart = _startTime.SecondsSince();
                            if (!_throttlingStats.ContainsKey(secondsSinceStart))
                            {
                                _throttlingStats.Add(secondsSinceStart, new ThrottlingData());
                            }
                            _throttlingStats[secondsSinceStart].ThrottleCount++;
                        }
                    }
                    break;
                case PrintStatsMessage printStatsMessage:
                    var now = DateTime.Now;
                    PrintProgress(now);

                    _dataProcessedSinceLastUpdate = 0;
                    _requestUnitsUsedSinceLastUpdate = 0;
                    _lastStatUpdate = now;
                    break;
                default:
                    break;
            }
        }

        private double GetThrottlingRatio()
        {
            var lastFiveSecondsStats = _throttlingStats.Where(s => s.Key >= (_startTime.SecondsSince() - 5)).ToList();
            var requestCount = lastFiveSecondsStats.Sum(s => s.Value.RequestCount);
            return requestCount == 0 ? 0 : (double)lastFiveSecondsStats.Sum(s => s.Value.ThrottleCount) / requestCount;
        }

        private void DispatchUploadRequests()
        {
            if (TryDispatchUploadRequest() && (_startTime.SecondsSince() > 5) && (!_options.NoConcurrency) && (_lastScaleOut.SecondsSince() > 2))
            {
                if (GetThrottlingRatio() == 0)
                {
                    if (TryDispatchUploadRequest())
                    {
                        _lastScaleOut = DateTime.Now;
                    }
                }
            }
        }

        private bool TryDispatchUploadRequest()
        {
            //if (_inFlightsDocs.Count > 0)
            //{
            //    Self.Tell(new UploadRequestMessage { Document = _inFlightsDocs.Dequeue() });
            //}
            //else
            //{
                var data = _fileReader.ReadLine();
                if (data != null)
                {
                    Document document = null;
                    using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(data)))
                    {
                        document = JsonSerializable.LoadFrom<Document>(memoryStream);
                    }
                    Self.Tell(new UploadRequestMessage { Document = document });
                }
                else
                {
                    return false;
                }
            //}

            return true;
        }

        private void UpdateStats(ResourceResponse<Document> resourceResponse)
        {
            _dataProcessed++;
            _dataProcessedSinceLastUpdate++;
            _requestUnitsUsedSinceLastUpdate += resourceResponse.RequestCharge;

            var secondsSinceStart = _startTime.SecondsSince();
            if (!_throttlingStats.ContainsKey(secondsSinceStart))
            {
                _throttlingStats.Add(secondsSinceStart, new ThrottlingData());
            }
            _throttlingStats[secondsSinceStart].RequestCount++;
        }

        private void PrintProgress(DateTime now)
        {
            var elapsedSinceLastUpdate = (now - _lastStatUpdate).TotalSeconds;
            Console.Write($"\rCompletion: {(float)_dataProcessed / _dataSize * 100:F2}% | {_inFlightRequests} in-flight request{(_inFlightRequests > 1 ? "s" : string.Empty)} | {GetThrottlingRatio():F2}% throttling | {(float)_dataProcessedSinceLastUpdate / elapsedSinceLastUpdate:F2} req/s | {_requestUnitsUsedSinceLastUpdate / elapsedSinceLastUpdate:F2} RU/s\t\t");
        }
    }

    public class Options
    {
        [Option('e', "endpoint")]
        public string Endpoint { get; set; }
        [Option('k', "key")]
        public string Key { get; set; }
        [Option('d', "database")]
        public string Database { get; set; }
        [Option('c', "collection")]
        public string Collection { get; set; }
        [Option('i', "inputFile")]
        public string InputFile { get; set; }
        [Option("no-concurrency")]
        public bool NoConcurrency { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var options = new Options();
            var optionsParseResult = Parser.Default.ParseArguments<Options>(args);
            if (optionsParseResult is NotParsed<Options> notParsedOptions)
            {
                return;
            }
            else if (optionsParseResult is Parsed<Options> parsedOptions)
            {
                if (!File.Exists(parsedOptions.Value.InputFile))
                {
                    return;
                }

                var system = ActorSystem.Create("CosmosBulkImport");
                var controllerRef = system.ActorOf(Props.Create(() => new BulkImporterActor(parsedOptions.Value)), "bulk-importer");
                controllerRef.Tell(new BulkImporterActor.StartMessage(), ActorRefs.NoSender);
            }

            Console.ReadLine();
        }
    }
}