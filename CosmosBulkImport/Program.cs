namespace CosmosBulkImport
{
    using Akka.Actor;
    using CommandLine;
    using System;
    using System.IO;

    static class DateTimeExtensions
    {
        public static int SecondsSince(this DateTime dateTime)
        {
            return (int)(DateTime.Now - dateTime).TotalSeconds;
        }
    }

    public class Options
    {
        [Option('e', "endpoint", Required = true, HelpText = "The endpoint of your Cosmos DB account")]
        public string Endpoint { get; set; }
        [Option('k', "key", Required = true, HelpText = "The authentication key")]
        public string Key { get; set; }
        [Option('d', "database", Required = true, HelpText = "The name of the database")]
        public string Database { get; set; }
        [Option('c', "collection", Required = true, HelpText = "The name of the collection where you want to upload your documents")]
        public string Collection { get; set; }
        [Option('i', "inputFile", Required = true, HelpText = "The path to the file containing the documents")]
        public string InputFile { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var optionsParseResult = Parser.Default.ParseArguments<Options>(args);
            if (optionsParseResult is NotParsed<Options> notParsedOptions)
            {
                return;
            }
            else if (optionsParseResult is Parsed<Options> parsedOptions)
            {
                if (!File.Exists(parsedOptions.Value.InputFile))
                {
                    Console.WriteLine("Can't find input file");
                    return;
                }

                var system = ActorSystem.Create("CosmosBulkImport");
                var controllerRef = system.ActorOf(Props.Create(() => new BulkImporterActor()), "bulk-importer");
                controllerRef.Tell(new BulkImporterActor.StartMessage(parsedOptions.Value), ActorRefs.NoSender);

                Console.ReadLine();
            }
        }
    }
}