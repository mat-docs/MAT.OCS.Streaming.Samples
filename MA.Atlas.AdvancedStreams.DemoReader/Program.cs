// <copyright file="Program.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MAT.OCS.Streaming;
using MAT.OCS.Streaming.Codecs.Protobuf;
using MAT.OCS.Streaming.IO;
using MAT.OCS.Streaming.Kafka;

namespace MA.Atlas.AdvancedStreams.DemoReader
{
    class Program
    {
        private const string KafkaBroker = "localhost:9092";
        private static readonly Uri DependeciesSvcUri = new Uri("http://localhost:8180/api/dependencies/");

        private static readonly DependencyClient DependencyClient = new HttpDependencyClient(DependeciesSvcUri, "dev");
        private static readonly DataFormatClient DataFormatClient = new DataFormatClient(DependencyClient);

        private static readonly ISet<string> Parameters = new HashSet<string>
        {
            "vCar:Chassis",
            "gLat:Chassis",
            "gLong:Chassis",
        };

        static void Main(string[] args)
        {
            ProtobufCodecs.RegisterCodecs();

            //Kafka can track the progress of each consumer
            using var kafkaClient = new KafkaStreamClient(KafkaBroker) { ConsumerGroup = "DemoReader" };

            using var streamPipeline = kafkaClient.StreamTopic("test").Into(InputFactory);

            if (!streamPipeline.WaitUntilConnected(TimeSpan.FromMinutes(1), CancellationToken.None))
                throw new InvalidOperationException("didn't connect");


            //var results = kafkaClient.ListTopics().Result;

            //foreach (var result in results)
            //{
            //    Console.WriteLine(result);
            //}

            //var topicSessions = kafkaClient.ListSessions("test");

            //foreach (var ts in topicSessions)
            //{
                
            //}
            Console.WriteLine("Ready");
            Console.ReadLine();
        }

        private static IStreamInput InputFactory(string streamId)
        {
            var input = new SessionTelemetryDataInput(streamId, DataFormatClient);

            input.SessionInput.SessionStateChanged += (sender, args) =>
            {
                Console.WriteLine($"{args.Session.Id} {args.Session.State}");
            };

            //With Protobuf
            //Under engineered:
            input.SamplesInput.AutoBindFeeds((sender, args) =>
            {
                var samples = args.Data;

                foreach (var (identifier, paramSamples) in samples.Parameters.Where(kv => Parameters.Contains(kv.Key)))
                {
                    var timeStamps = paramSamples.TimestampsNanos;
                    var values = paramSamples.Values;

                    for (var t = 0; t < timeStamps.Length; t++)
                    {
                        Console.WriteLine($"{identifier} {timeStamps[t]}, {values[t]}");
                    }
                }
            });

            //Other way of doing it (json)
            //Over engineered:
            //input.DataInput.BindDefaultFeed("gLat:Chassis", "gLong:Chassis").DataBuffered += (sender, args) =>
            //{
            //    var data = args.Buffer.GetData();

            //    var timestamps = data.TimestampsNanos;
            //    var gLat = data.Parameters[0].AvgValues;
            //    var gLong = data.Parameters[1].AvgValues;

            //    for (var t = 0; t < timestamps.Length; t++)
            //    {
            //        Console.WriteLine($"{timestamps[t]}, {gLat[t]}, {gLong[t]}");
            //    }
            //};

            return input;
        }
    }
}