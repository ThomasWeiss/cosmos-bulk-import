namespace CosmosBulkImport
{
    using Akka.Actor;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    static class DateTimeExtensions
    {
        public static int SecondsSince(this DateTime dateTime)
        {
            return (int)(DateTime.Now - dateTime).TotalSeconds;
        }
    }

    public class BulkImporterActor : UntypedActor
    {
        // these are the messages that can be received by our actor:
        // - the initial startup message sent after the actor is created
        public class StartMessage
        {
            public StartMessage(Options options) => Options = options;
            public Options Options { get; }
        }

        // - a request to upload a document
        public class UploadRequestMessage
        {
            public UploadRequestMessage(Document document) => Document = document;
            public Document Document { get; }
        }

        // - a notification that an upload has succeeded
        public class UploadSucceededMessage
        {
            public UploadSucceededMessage(ResourceResponse<Document> resourceResponse) => ResourceResponse = resourceResponse;
            public ResourceResponse<Document> ResourceResponse { get; }
        }

        // - a notification that an upload has failed
        public class UploadFailedMessage
        {
            public UploadFailedMessage(Document document, Exception exception)
            {
                Document = document;
                Exception = exception;
            }
            public Document Document { get; }
            public Exception Exception { get; }
        }

        // - a request to print stats to the console
        public class PrintStatsMessage { }

        private int _dataSize = 0;
        private int _dataProcessed = 0;
        private int _inFlightRequests = 0;

        class RequestData
        {
            public int RequestCount { get; set; }
            public int ThrottleCount { get; set; }
        }
        private Dictionary<int, RequestData> _requestStats = new Dictionary<int, RequestData>();
        private Queue<Document> _failedUploads = new Queue<Document>();

        private DateTime _startTime;
        private DateTime _lastStatUpdate;
        private DateTime _lastConcurrencyBoost;
        private Cancelable _schedulerCanceler;
        private int _dataProcessedSinceLastStatsUpdate = 0;
        private double _requestUnitsUsedSinceLastStatsUpdate = 0;

        private IFileReader _fileReader;
        private DocumentClient _documentClient;
        private Uri _collectionUri;

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case StartMessage startMessage:
                    Console.WriteLine($"Uploading documents from {Path.GetFileName(startMessage.Options.InputFile)} to {startMessage.Options.Endpoint}/{startMessage.Options.Database}/{startMessage.Options.Collection}...");

                    if (!SetupFileReader(startMessage.Options))
                    {
                        Console.WriteLine("Invalid input file, press Enter to quit");
                        Context.Stop(Self);
                        return;
                    }
                    SetupDocumentClient(startMessage.Options);
                    SetupStats();

                    DispatchUploadRequests();
                    break;
                case UploadRequestMessage uploadRequestMessage:
                    _documentClient.UpsertDocumentAsync(_collectionUri, uploadRequestMessage.Document)
                        .PipeTo(Self, null, 
                            resourceResponse => new UploadSucceededMessage(resourceResponse), 
                            exception => new UploadFailedMessage(uploadRequestMessage.Document, exception.InnerException));
                    _inFlightRequests++;
                    break;
                case UploadSucceededMessage uploadSucceededMessage:
                    _inFlightRequests--;
                    UpdateRequestStats(uploadSucceededMessage.ResourceResponse);

                    if (_dataProcessed < _dataSize)
                    {
                        DispatchUploadRequests();
                    }
                    else
                    {
                        Terminate();
                    }
                    break;
                case UploadFailedMessage uploadFailedMessage:
                    _inFlightRequests--;
                    if ((uploadFailedMessage.Exception is DocumentClientException ex) && ((int)ex.StatusCode == 429))
                    {
                        // the request has been throttled by the service
                        UpdateThrottleStats();
                    }
                    // enqueue the document so we upload it again on a subsequent request
                    _failedUploads.Enqueue(uploadFailedMessage.Document);

                    DispatchUploadRequests();
                    break;
                case PrintStatsMessage printStatsMessage:
                    var now = DateTime.Now;
                    PrintProgress(now);

                    _dataProcessedSinceLastStatsUpdate = 0;
                    _requestUnitsUsedSinceLastStatsUpdate = 0;
                    _lastStatUpdate = now;
                    break;
                default:
                    break;
            }
        }

        private bool SetupFileReader(Options options)
        {
            _fileReader = new FileReader();
            _fileReader.Load(options.InputFile);
            if (_fileReader.Validate())
            {
                _dataSize = _fileReader.GetDocumentCount();
                return true;
            }
            else
            {
                return false;
            }
        }

        private void SetupDocumentClient(Options options)
        {
            _documentClient = new DocumentClient(new Uri(options.Endpoint), options.Key, new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                MaxConnectionLimit = int.MaxValue,
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 0
                }
            });
            _collectionUri = UriFactory.CreateDocumentCollectionUri(options.Database, options.Collection);
        }

        private void SetupStats()
        {
            _startTime = DateTime.Now;
            _lastStatUpdate = DateTime.Now;
            _lastConcurrencyBoost = DateTime.Now;
            _schedulerCanceler = new Cancelable(Context.System.Scheduler);
            Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0), TimeSpan.FromSeconds(1), Self, new PrintStatsMessage(), ActorRefs.NoSender, _schedulerCanceler);
        }

        private void DispatchUploadRequests()
        {
            var throttlingRatio = GetThrottlingRatio(1);
            // we only dispatch a new request if there are zero in-flight requests or if the current throttling ratio is below 10%
            if ((_inFlightRequests == 0) || (throttlingRatio < 0.1))
            {
                if (TryDispatchUploadRequest())
                {
                    // if the throttling ratio is null and we haven't added concurrency in the last 2 seconds, launch another upload
                    if ((_startTime.SecondsSince() > 5) && (_lastConcurrencyBoost.SecondsSince() > 2) && (GetThrottlingRatio(1) == 0))
                    {
                        if (TryDispatchUploadRequest())
                        {
                            _lastConcurrencyBoost = DateTime.Now;
                        }
                    }
                }
            }
        }

        private bool TryDispatchUploadRequest()
        {
            if (_failedUploads.Count > 0)
            {
                // if there are documents that failed to upload, we try them again
                Self.Tell(new UploadRequestMessage(_failedUploads.Dequeue()));
            }
            else
            {
                // read next document from input file
                var document = _fileReader.GetNextDocument();
                if (document != null)
                {
                    Self.Tell(new UploadRequestMessage(document));
                }
                else
                {
                    // we've exhausted all available data
                    return false;
                }
            }

            return true;
        }

        private void UpdateRequestStats(ResourceResponse<Document> resourceResponse)
        {
            _dataProcessed++;
            _dataProcessedSinceLastStatsUpdate++;
            _requestUnitsUsedSinceLastStatsUpdate += resourceResponse.RequestCharge;

            var secondsSinceStart = _startTime.SecondsSince();
            if (!_requestStats.ContainsKey(secondsSinceStart))
            {
                _requestStats.Add(secondsSinceStart, new RequestData());
            }
            _requestStats[secondsSinceStart].RequestCount++;
        }

        private void UpdateThrottleStats()
        {
            var secondsSinceStart = _startTime.SecondsSince();
            if (!_requestStats.ContainsKey(secondsSinceStart))
            {
                _requestStats.Add(secondsSinceStart, new RequestData());
            }
            _requestStats[secondsSinceStart].ThrottleCount++;
        }

        private double GetThrottlingRatio(int nbSeconds)
        {
            var lastStats = _requestStats.Where(s => s.Key >= (_startTime.SecondsSince() - nbSeconds)).ToList();
            var requestCount = lastStats.Sum(s => s.Value.RequestCount);
            return requestCount == 0 ? 0 : (double)lastStats.Sum(s => s.Value.ThrottleCount) / requestCount;
        }

        private void PrintProgress(DateTime now)
        {
            var completion = (float)_dataProcessed / _dataSize;
            var elapsedSinceLastUpdate = (now - _lastStatUpdate).TotalSeconds;

            // erase console output
            Console.Write($"\r{new String(' ', Console.WindowWidth - 1)}\r");
            Console.Write($"[{new String('#', (int)(completion * 20))}{new String(' ', 20 - (int)(completion * 20))}] {(int)(completion * 100)}% | {_inFlightRequests} in-flight request{(_inFlightRequests > 1 ? "s" : string.Empty)} | {GetThrottlingRatio(1) * 100:F2}% throttling | {(float)_dataProcessedSinceLastStatsUpdate / elapsedSinceLastUpdate:F2} req/s | {_requestUnitsUsedSinceLastStatsUpdate / elapsedSinceLastUpdate:F2} RU/s");
        }

        private void Terminate()
        {
            _schedulerCanceler.Cancel();
            Context.Stop(Self);

            // erase console output
            Console.Write($"\r{new String(' ', Console.WindowWidth - 1)}\r");
            Console.WriteLine($"{_dataSize} documents uploaded in {(DateTime.Now - _startTime).TotalSeconds:F0} seconds, press Enter to quit");
        }
    }
}