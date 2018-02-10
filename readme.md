# Cosmos DB Bulk Importer

This is a PoC showing how you can leverage an actor framework like [Akka.Net](http://getakka.net/) to easily handle concurrent uploads to [Azure Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/).

## How to use it

This application is a .NET Core console app, so you can launch it by calling ` dotnet CosmosBulkImport.dll`. It requires 5 parameters:

- `--endpoint` or `-e`: the endpoint of your Cosmos DB account (i.e. `https://xxx.documents.azure.com:443/`)
- `--key` or `-k`: the authentication key
- `--database` or `-d`: the name of the database
- `--collection` or `-c`: the name of the collection where you want to upload your documents
- `--inputFile` or `-i`: the path to the file containing the documents

### File format

The original implementation expects a text file containing:
- the total count of documents on the first line
- a JSON-encoded string of each document on the following lines

Extracting the documents from the input file is conveniently isolated in a `IFileReader` interface so it should be easy to update it to your own format.

### Output

While running, the importer displays the progress, number of in-flight requests, throttling ratio, request rate and an average of RU/s consumed:

![example output](https://raw.githubusercontent.com/ThomasWeiss/cosmos-bulk-import/master/screenshots/example.jpg)

## Background

When you have to upload a massive amount of data to a Cosmos DB collection, you basically have to deal with 3 limiting factors:
1. the rate at which Cosmos DB can ingest the data
2. the network bandwidth between you and the Azure datacenter hosting your Cosmos DB instance
3. the rate at which your machine can dispatch upload requests

#1 is easy to control as Cosmos DB provides elastic throughput. You can crank up your provisioned throughput to millions of [Request Units](https://thomasweiss.io/how-i-learned-to-stop-worrying-and-love-cosmos-dbs-request-units/) per second just for the duration of your import operation (remember though that RU billing is per hour).

#2 is something you probably can't really control.

#3 is where this little project comes into play. In order to use #1 and #2 to their full extent, you want your client code to issue requests as fast as possible. But if you issue too many requests and hit the limit of the performance you've provisioned on your Cosmos DB container, you will get throttled by the service. So what you really want is to issue just as many requests as your container can handle, and constantly monitor how much you're being throttled so you don't go beyond that limit and issue too many requests that would be refused.

## Implementation

I've used an actor framework to implement this tool and I will explain below why I've made that choice. But first, let's go through the high-level logic.

On startup, we issue a first upload request. Every time we get a response, successful or not, we go through this little algorithm:

- if the current throttling ratio is below 10%
  * issue an other upload request
  * if the current throttling ratio is null *and* we haven't increased concurrency for more than 2 seconds
    - issue an other upload request

In other words, the logic is:

- issue follow-up requests as long as the throttling ratio is below 10%
- if the throttling ratio is null, add concurrency by issuing a second request on top of the first one

To put it even differently, you can imagine our upload requests as forming chains. Every time a request completes, we follow-up with a new request and if the throttling ratio is null, we issue a second request, starting a new, parallel chain. This process is safeguarded by the first rule that breaks the chain if the throttling ratio becomes too high.

## Why an actor framework

My first intuition was to implement this by using threads, starting a new thread every time I increase the concurrency and create a new request chain. But then I realized that those threads would just `await` I/O calls (the calls to the Cosmos DB service) and remembered a conversation I had with fellow developers some time ago. We were discussing the fact that **scaling out a workload that is made of concurrent I/O does not require threads**. C#'s `async`/`await` already does the job of delegating these I/O calls to background threads, so all we really have to do is to **orchestrate** these calls.

So the real challenge was to devise a solution that would juggle with multiple, concurrent `async` calls and handle them in a robust way. From there, it became obvious that an implementation based on actors would perfectly fit.

This is not the place to explain all the concepts behind [actors](https://en.wikipedia.org/wiki/Actor_model). Here are just the main attributes that are useful in this project:

- an actor can basically be seen as an instance of an object
- you cannot directly call methods on that object; instead, you send it *messages* that the actor receives from a queue
- an actor is inherently single-threaded, so:
  * it only processes one message at a time
  * it cannot be preempted while it processes a message, which means that **all its data can be considered immutable** (i.e. its data won't be updated by anything else than your message-processing code)

Using the Akka.Net actor framework, I've just implemented a single actor that is in charge of uploading the data to Cosmos DB. Every time a request completes, it comes back as a message sent to the actor. The actor can then process that response without being disturbed by other responses coming in the meantime, because these responses will be queued and delivered only once the current response has been completely processed.

To be honest, it took me quite some time to finalize the implementation of that actor because it's not trivial to wrap one's head around the actor runtime model. But once you get used to the idea that your message handling code is completely safe from concurrent interruptions, it becomes incredibly powerful.

## Results

The tool does its best to upload documents as fast as possible without exceeding too much the performance limit set on your Cosmos DB collection. Here it uploads to a collection with a limit of 1,000 RU/s:

![example output](https://raw.githubusercontent.com/ThomasWeiss/cosmos-bulk-import/master/screenshots/at1000rus.jpg)

Here's the same data uploaded to the same collection with a limit increased to 2,500 RU/s:

![example output](https://raw.githubusercontent.com/ThomasWeiss/cosmos-bulk-import/master/screenshots/at2500rus.jpg)

## Why not Orleans?

[Orleans](https://dotnet.github.io/orleans/) is an other actor framework, originally developed by Microsoft and now open-source. In a nutshell, the reason why I went for Akka.Net is because Orleans is distributed in nature and designed to run your actors over a cluster of machines; embedding it in a console application would require quite a lot of ceremony and using Akka.Net in that kind of context was just much more suitable.

## Gotchas

In order to have the fastest connection to your Cosmos DB database, the current implementation uses the [`Direct` connection mode over TCP](https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips#networking). This is because using HTTP can be limited by the number of outgoing connections allowed to the process, and the implementation of `ServicePointManager` in .NET Core is currently incomplete. This means that it will not work if your machine cannot establish a direct connection to your Cosmos DB database.

## Possible improvements

It's certainly possible to improve this implementation, notably by fine-tuning the heuristics used to decide whether to issue new requests or not. Don't hesitate to play with the throttling ratio thresholds and the time window over which the throttling ratio is computed.

Also, I must confess that my familiarity with Akka.Net is quite limited and my implementation may look hair-raising to Akka.Net specialists! :) I'm obviously open to any suggestions so feel free to point out any mistake I may have made here.