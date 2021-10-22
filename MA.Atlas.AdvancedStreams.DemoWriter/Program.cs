// <copyright file="Program.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using System;
using System.Threading;
using MAT.OCS.Streaming;
using MAT.OCS.Streaming.IO;
using MAT.OCS.Streaming.IO.TelemetryData;
using MAT.OCS.Streaming.Kafka;
using MAT.OCS.Streaming.Model;
using MAT.OCS.Streaming.Model.AtlasConfiguration;
using MAT.OCS.Streaming.Model.DataFormat;
using Range = MAT.OCS.Streaming.Model.AtlasConfiguration.Range;

namespace MA.Atlas.AdvancedStreams.DemoWriter
{
    //This is an example on how Atlas can ingest data from different sources (here, just dummy sine data we generate)
    class Program
    {
        private static readonly Random Rng = new Random();

        private const string KafkaBroker = "localhost:9092";
        private static readonly Uri DependeciesSvcUri = new Uri("http://localhost:8180/api/dependencies/");

        private static readonly DependencyClient
            DependencyClient = new HttpDependencyClient(DependeciesSvcUri, "dev"); //talk to dependency service

        private static readonly DataFormatClient DataFormatClient = new DataFormatClient(DependencyClient); //

        private static readonly AtlasConfigurationClient AtlasConfigurationClient =
            new AtlasConfigurationClient(DependencyClient);

        static void Main(string[] args)
        {
            using var kafkaClient = new KafkaStreamClient(KafkaBroker) { ConsumerGroup = "DemoWriter" };
            using var outputTopic = kafkaClient.OpenOutputTopic("Data");

            MakeData(outputTopic);

            Console.ReadLine();
        }

        //Let's pretend this is data coming from the outside:
        private static void MakeData(IOutputTopic outputTopic)
        {
            var dataFormat = DataFormat.DefineFeed().Parameters("p1:test", "p2:test").AtFrequency(100)
                .BuildFormat();
            var dataFormatId = DataFormatClient.PutAndIdentifyDataFormat(dataFormat);

            var atlasConfig = new AtlasConfiguration
            {
                AppGroups =
                {
                    ["test"] = new ApplicationGroup
                    {
                        Groups =
                        {
                            ["group"] = new ParameterGroup
                            {
                                Parameters =
                                {
                                    ["p1:test"] = new Parameter
                                    {
                                        Name = "p1_test",
                                        Description = "p1 test",
                                        PhysicalRange = new Range(-1000, 1000),
                                        WarningRange = new Range(-1000, 1000)
                                    },
                                    ["p2:test"] = new Parameter
                                    {
                                        Name = "p2_test",
                                        Description = "p2 test",
                                        PhysicalRange = new Range(-1000, 1000),
                                        WarningRange = new Range(-1000, 1000)
                                    }
                                }
                            }
                        }
                    }
                }
            };

             var atlasConfigId = AtlasConfigurationClient.PutAndIdentifyAtlasConfiguration(atlasConfig);

            var output = new SessionTelemetryDataOutput(outputTopic, dataFormatId, DataFormatClient);
            var feed = output.DataOutput.BindDefaultFeed();

            var start = DateTime.Now;
            var epoch = start.Date.ToTelemetryTime();
            var offsetMillis = (long)start.TimeOfDay.TotalMilliseconds;

            output.SessionOutput.SessionIdentifier = "Workshop signals";
            output.SessionOutput.AddSessionDependency(DependencyTypes.DataFormat, dataFormatId);
            output.SessionOutput.AddSessionDependency(DependencyTypes.AtlasConfiguration, atlasConfigId);
            output.SessionOutput.SessionStart = start;
            output.SessionOutput.SessionState = StreamSessionState.Open;
            output.SessionOutput.SendSession(); //don't wait on the task to come back (generally not what you want to do see ~1:10 in the video)

            const int durationMs = 240000; //4mins

            const int samplesPerStep = 10;
            const int intervalMs = 10;

            var timestamps = new long[samplesPerStep];
            var p1 = new double[samplesPerStep];
            var p2 = new double[samplesPerStep];

            var statuses = new DataStatus[samplesPerStep];
            Array.Fill(statuses, DataStatus.Sample);

            var gen1 = new TestSignalGenerator(Rng);
            var gen2 = new TestSignalGenerator(Rng);

            for (var ms = 0; ms < durationMs; ms += (samplesPerStep * intervalMs))
            {
                for (var t = 0; t < samplesPerStep; t++)
                {
                    var time = ms + (t * intervalMs) + offsetMillis;

                    timestamps[t] = time.MillisToNanos();
                    p1[t] = gen1[time];
                    p2[t] = gen2[time];

                    //Console.WriteLine($"{time}, {p1[t]}, {p2[t]}");
                }

                var data = new TelemetryData
                {
                    TimestampsNanos = timestamps,
                    EpochNanos = epoch,
                    Parameters = new []
                    {
                        new TelemetryParameterData
                        {
                            AvgValues = p1,
                            Statuses = statuses
                        },
                        new TelemetryParameterData
                        {
                            AvgValues = p2,
                            Statuses = statuses
                        }
                    }
                };

                feed.EnqueueAndSendData(data);

                var sleep = (start + TimeSpan.FromMilliseconds(ms)) - DateTime.Now;
                if (sleep > TimeSpan.Zero)
                {
                    Thread.Sleep(sleep);
                }

                output.SessionOutput.SessionDurationNanos = Time.MillisToNanos(ms);

                Console.Write(".");
            }

            output.SessionOutput.SessionState = StreamSessionState.Closed;
            output.SessionOutput.SendSession();
        }
    }
}