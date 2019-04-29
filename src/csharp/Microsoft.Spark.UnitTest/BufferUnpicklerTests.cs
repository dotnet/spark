using System.Linq;
using Razorvine.Pickle;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class BufferUnpicklerTests
    {
        [Fact]
        public void CanDeserializeArrayOfDoubles()
            => AssertDeserializationResultIsTheSameAsSerializedInput(
                Enumerable.Range(0, 100).Select(x => (object)(x * 0.5)).ToArray());

        [Fact]
        public void CanDeserializeArrayOfIntegers()
            => AssertDeserializationResultIsTheSameAsSerializedInput(
                Enumerable.Range(0, 100).Select(x => (object)(int)(x)).ToArray());

        [Fact]
        public void CanDeserializeArrayOfStrings()
            => AssertDeserializationResultIsTheSameAsSerializedInput(
                Enumerable.Range(0, 100).Select(x => (object)(x.ToString())).ToArray());

        private void AssertDeserializationResultIsTheSameAsSerializedInput(object[] input)
        {
            var pickler = new Pickler();

            byte[] serialized = pickler.dumps(input);

            object[] deserialized = (object[])new BufferUnpickler().load(serialized);

            Assert.Equal(input, deserialized);
        }
    }
}
