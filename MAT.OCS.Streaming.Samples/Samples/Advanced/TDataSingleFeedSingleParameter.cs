using MAT.OCS.Streaming.IO.TelemetryData;
using MAT.OCS.Streaming.Model.AtlasConfiguration;
using MAT.OCS.Streaming.Model.DataFormat;
using System;
using MAT.OCS.Streaming.Model;
using MAT.OCS.Streaming.Samples.Adapters;
using System.Threading;

namespace MAT.OCS.Streaming.Samples.Samples
{
    class TDataSingleFeedSingleParameter
    {
        // The data tree structure in Atlas is built up by these values, and would look like this:
        // Chassis 
        //        |- State 
        //               |- vCar
        //                      (vCar:Chassis, kmh, ...)
        private const string AppGroupId = "Chassis";
        private const string ParameterGroupId = "State";
        private const string ParameterId = "vCar:Chassis";
        private const string ParameterName = "vCar";
        private const string ParameterUnits = "kmh";

        private const double Frequency = 100; // The frequency used in the DataFormat. The default frequency is 100 anyway.
        private const long Interval = (long)(1000 / Frequency * 1000000L); // interval in nanoseconds, calculated by the given frequency

        // A single group and single parameter Atlas configuration sample.
        public static AtlasConfiguration AtlasConfiguration = new AtlasConfiguration
        {
            AppGroups =
            {
                {
                    AppGroupId, new ApplicationGroup
                    {
                        Groups =
                        {
                            {
                                ParameterGroupId, new ParameterGroup
                                {
                                    Parameters =
                                    {
                                        {
                                            ParameterId,
                                            new Parameter {Name = ParameterName, Units = ParameterUnits}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        /// <summary>
        /// Creates a DataFormat object from the default feed and the given frequency and parameterId.
        /// </summary>
        /// <returns>The Dataformat object built by the configured DataformatBuilder.</returns>
        public static DataFormat GetDataFormat()
        {
            // Data formats can contain multiple feeds, each feed has a frequency, which is 100 by default.
            // Name is optional, but if you wish to define multiple feeds, give them names
            // else previous unnamed feed will be overwritten. The default is the empty string.
            var dataFormatBuilder = DataFormat.DefineFeed();

            // Setting frequency to the previously started feed. This can be only one value, if you set
            // it multiple times, previous value will be overwritten. If you wish to use multiple
            // frequencies, use new feed by calling .NextFeed()
            dataFormatBuilder.AtFrequency(Frequency);

            // Adding parameter to the previously started feed by its parameterId.
            dataFormatBuilder.Parameter(ParameterId); 

            var dataFormat = dataFormatBuilder.BuildFormat();

            return dataFormat;
        }

        /// <summary>
        /// Generates random TelemetryData. 
        /// </summary>
        /// <param name="sampleCount">The sample count that is used to set the size of each TelemetryData.Parameter values' size.</param>
        /// <param name="sessionStart">Used to set the EpochNanos property of the TelemetryData. TelemetryData.Parameters
        /// share the same timestamps as they aligned to the same time.</param>
        /// <returns></returns>
        public static TelemetryData GenerateData(int sampleCount, DateTime sessionStart)
        {
            var data = new TelemetryData()
            {
                EpochNanos = sessionStart.ToTelemetryTime(),
                TimestampsNanos = new long[sampleCount],
                Parameters = new TelemetryParameterData[1] // The sample AtlasConfiguration has only 1 parameter
            };

            // The sample AtlasConfiguration has only 1 parameter, so data.Parameters[0] must be initialized only.
            data.Parameters[0] = new TelemetryParameterData()
            {
                AvgValues = new double[sampleCount], // The data will be stored in the AvgValues array, so this must be initialized
                Statuses = new DataStatus[sampleCount] // Status is stored for each data, so Statuses array must be initialized
            };

            var randomRangeWalker = new RandomRangeWalker(0, 1); // Used to generated random data

            for (int i = 0; i < sampleCount; i++)
            {
                var nextData = randomRangeWalker.GetNext();
                data.TimestampsNanos[i] = i * Interval; // timestamps expressed in ns since the epoch, which is the start of the session
                data.Parameters[0].AvgValues[i] = nextData;
                data.Parameters[0].Statuses[i] = DataStatus.Sample;
            }

            return data;
        }

        public static void Write()
        {
            var dependencyUri = new Uri("http://localhost:8180/api/dependencies/"); // The URI where the dependency services are running
            const string brokerList = "localhost:9092"; // The host and port where the Kafka broker is running
            const string groupName = "dev"; // The group name
            const string topicName = "sample_in"; // The existing topic's name in the Kafka broker. The *_announce topic name must exist too. In this case the sample_in_announce
            var streamAdapter = new KafkaStreamAdapter(brokerList, "writeConsumerGroup"); // The steam adapter used to manage streams and topics in the broker. Consumer group must be unique for each stream.
            using (var topicOutput = streamAdapter.OpenOutputTopic(topicName)) // Open a KafkaOutputTopic
            {
                using (var writer = new Writer(dependencyUri, AtlasConfiguration, GetDataFormat(), groupName, topicOutput)) // Create a Writer object that is used to manage the session and write data
                {
                    var feedName = ""; // As sample DataFormat uses default feed, we will leave this empty.
                    writer.OpenSession("sample_" + DateTime.Now); // The session must be opened before Write

                    for (int i = 0; i < 5; i++) // Write 5 times within the session, but different data.
                    {
                        var generatedData = GenerateData(10, (DateTime)writer.SessionStart); // Generate some TelemetryData
                        writer.Write(feedName, generatedData); // Write the TelemetryData to the given feed (default "" in this case)
                    }
                    writer.CloseSession(); // The session must be closed after writing, otherwise it would be marked as Truncated, which is used as an error flag.
                }
            }
        }

        public static void Read()
        {
            var dependencyUri = new Uri("http://localhost:8180/api/dependencies/"); // The URI where the dependency services are running
            const string brokerList = "localhost:9092"; // The host and port where the Kafka broker is running
            const string groupName = "dev"; // The group name
            const string topicName = "sample_in"; // The existing topic's name in the Kafka broker. The *_announce topic name must exist too. In this case the sample_in_announce
            var streamAdapter = new KafkaStreamAdapter(brokerList, "writeConsumerGroup"); // The steam adapter used to manage streams and topics in the broker. Consumer group must be unique for each stream.
            var stream = streamAdapter.OpenStreamTopic(topicName); // Open the topic for streaming.
            using (var reader = new Reader(dependencyUri, groupName, stream)) // Create a Reader to read from the stream
            {
                using (var pipeline = reader.ReadTData(ParameterId, Models.TraceData)) // TelemetryDataHandler parameter can be used to handle the data read from the stream.
                {
                    Write(); //Write some data to have something to read while connection is open.
                }
            }
        }

        public static void ReadAndLink()
        {
            var dependencyUri = new Uri("http://localhost:8180/api/dependencies/"); // The URI where the dependency services are running
            const string brokerList = "localhost:9092"; // The host and port where the Kafka broker is running
            const string groupName = "dev"; // The group name
            const string readTopicName = "sample_in"; // The existing topic's name in the Kafka broker. The *_announce topic name must exist too. In this case the sample_in_announce
            const string linkTopicName = "sample_out"; // The existing topic's name in the Kafka broker. The *_announce topic name must exist too. In this case the sample_out_announce
            var readAdapter = new KafkaStreamAdapter(brokerList, "readConsumerGroup"); // The steam adapter used to manage streams and topics in the broker. Consumer group must be unique for each stream.
            var writeAdapter = new KafkaStreamAdapter(brokerList, "writeConsumerGroup"); // The steam adapter used to manage streams and topics in the broker. Consumer group must be unique for each stream.
            var stream = readAdapter.OpenStreamTopic(readTopicName); // Open the topic for streaming.

            using (var reader = new Reader(dependencyUri, groupName, stream)) // Create a Reader to read from the stream
            {
                using (var outputTopic = writeAdapter.OpenOutputTopic(linkTopicName)) // Open the output topic, where you want to link the streamed input
                {
                    using (var writer = new Writer(dependencyUri, AtlasConfiguration, GetDataFormat(), groupName, outputTopic)) // Create a Writer for the output topic and pass
                    {
                        const string outputFeedName = ""; // As sample DataFormat uses default feed, we will leave this empty.
                        using (IStreamPipeline pipeline = reader.ReadAndLinkTData(ParameterId, Models.TraceData, writer, outputFeedName)) // TelemetryDataHandler parameter can be used to handle the data read from the stream.
                        {
                            Thread.Sleep(5000); // NOTE: without this doesn't seem to work
                            Write(); //Write some data to have something to read while connection is open.
                        }
                    }
                }
            }
        }
    }
}
