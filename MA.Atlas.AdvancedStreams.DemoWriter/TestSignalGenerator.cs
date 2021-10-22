// <copyright file="TestSignalGenerator.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using System;

namespace MA.Atlas.AdvancedStreams.DemoWriter
{
    public class TestSignalGenerator
    {
        private struct Component
        {
            public double IntervalSec;
            public double TimeOffsetSec;
            public double Amplitude;
            public double ValueOffset;
        }

        private const int MinComponents = 1;
        private const int MaxComponents = 3;
        private const double MinFrequency = 0.01;
        private const double MaxFrequency = 1.0;
        private const double MaxTimeOffset = 10.0;
        private const double MaxAmplitude = 500.0;
        private const double MaxValueOffset = 500.0;

        private readonly Component[] components;

        public TestSignalGenerator(Random rng)
        {
            components = new Component[rng.Next(MaxComponents - MinComponents + 1) + MinComponents];

            var maxAmplitude = MaxAmplitude;
            var maxFrequency = MaxFrequency;
            for (var i = 0; i < components.Length; i++)
            {
                var amplitude = maxAmplitude = rng.NextDouble() * maxAmplitude;
                var frequency = maxFrequency = rng.NextDouble() * (maxFrequency - MinFrequency) + MinFrequency;

                components[i] = new Component
                {
                    Amplitude = amplitude,
                    IntervalSec = 1.0 / frequency,
                    TimeOffsetSec = rng.NextDouble() * MaxTimeOffset,
                    ValueOffset = rng.NextDouble() * (MaxValueOffset * 2) - MaxValueOffset
                };
            }
        }

        public double this[long timeMillis]
        {
            get
            {
                var signal = 0.0;
                for (var i = 0; i < components.Length; i++)
                {
                    var component = components[i];
                    var offsetTime = timeMillis / 1000.0 + component.TimeOffsetSec;
                    var value = Math.Sin(2 * Math.PI * offsetTime / component.IntervalSec) * component.Amplitude + component.ValueOffset;
                    signal += value;
                }
                return signal;
            }
        }
    }
}