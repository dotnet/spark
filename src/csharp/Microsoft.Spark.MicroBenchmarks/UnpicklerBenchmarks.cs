using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using Microsoft.Spark.IO;

namespace Microsoft.Spark.MicroBenchmarks
{
    public class UnpicklerBenchmarks
    {
        private MemoryStream _inputStream;
        private MaxLengthReadStream _maxLengthStream;
        private byte[] _bytes;

        [GlobalSetup]
        public void SetupDeserializeRealInput()
        {
            // the content of serializedSampleInput.txt is a Base64 serialized content captured from the Q1 benchmark
            var buildOutputDirectory = Path.GetDirectoryName(typeof(UnpicklerBenchmarks).Assembly.Location);
            var inputFilePath = Path.Combine(buildOutputDirectory, "Resources", "serializedSampleInput.txt");

            _bytes = Convert.FromBase64String(File.ReadAllText(inputFilePath));
            _inputStream = new MemoryStream(_bytes);
            _maxLengthStream = new MaxLengthReadStream();
        }

        [Benchmark(Baseline = true)]
        public object[] GetUnpickledObjectsFromMemoryStream()
        {
            _inputStream.Position = 0;

            return Utils.PythonSerDe.GetUnpickledObjects(_inputStream);
        }

        [Benchmark]
        public object[] GetUnpickledObjectsFromMaxLengthReadStream()
        {
            _inputStream.Position = 0;
            _maxLengthStream.Reset(_inputStream, _bytes.Length);

            return Utils.PythonSerDe.GetUnpickledObjects(_maxLengthStream);
        }

        [Benchmark]
        public object[] GetUnpickledObjectsFromBuffer() => Utils.PythonSerDe.GetUnpickledObjects(_bytes);
    }
}
