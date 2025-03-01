// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using MessagePack;
using MessagePack.Resolvers;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Utils;

// If deserialization of untrusted data is required, extend this functionality to
// incorporate techniques such as using a Message Authentication Code (MAC)
// or whitelisting allowed types to mitigate security risks.

/// <summary>
/// BinarySerDe (Serialization/Deserialization) is a utility class designed to handle
/// serialization and deserialization of objects to and from binary formats.
///
/// <para>
/// This implementation uses the MessagePack `Typeless` API, which embeds type
/// information into the serialized data. It adds overhead of 1-2 bytes for primitive
/// types, and serializes 'System.Type' entirely for complex objects.
/// Does not serialize type definition, so in order to deserialize a complex object,
/// declaring library should be available in app domain or at probing locations.
/// </para>
/// </summary>
internal static class BinarySerDe
{
    private static MessagePackSerializerOptions _options =
        new AllowStandardOrSerializableMessagePackSerializerOptions(
            TypelessContractlessStandardResolver.Instance
        ).WithSecurity(MessagePackSecurity.UntrustedData);

    /// <summary>
    /// Deserializes a stream of binary data into an object of type T.
    /// When using or shared streams, prefer the overloaded version
    /// that accepts a `length` parameter to ensure no excess data is consumed.
    /// </summary>
    /// <typeparam name="T">The expected type of the deserialized object.</typeparam>
    /// <param name="stream">The stream containing the serialized data.</param>
    /// <returns>An object of type T.</returns>
    internal static T Deserialize<T>(Stream stream)
    {
        return (T)MessagePackSerializer.Typeless.Deserialize(stream, _options);
    }

    /// <summary>
    /// Deserializes an object from stream, ensuring no excess data is read.
    /// </summary>
    /// <param name="stream">The stream containing the serialized data.</param>
    /// <param name="length">The length of byte section to deserialize.</param>
    /// <returns>The deserialized object.</returns>
    internal static object Deserialize(Stream stream, int length)
    {
        ReadOnlyMemory<byte> memory = SerDe.ReadBytes(stream, length);

        return MessagePackSerializer.Typeless.Deserialize(memory, _options);
    }

    /// <summary>
    /// Serializes an object into a binary stream
    /// </summary>
    /// <typeparam name="T">The type of the object to serialize.</typeparam>
    /// <param name="stream">The target stream where the data will be written.</param>
    /// <param name="graph">The object to serialize.</param>
    internal static void Serialize<T>(Stream stream, T graph)
    {
        MessagePackSerializer.Typeless.Serialize(stream, graph, _options);
    }
}

/// <summary>
/// Additional security for MessagePack typeless serialization, that only allows
/// standard classes or classes marked with the 'System.Serializable' attribute.
/// </summary>
internal class AllowStandardOrSerializableMessagePackSerializerOptions
    : MessagePackSerializerOptions
{
    public AllowStandardOrSerializableMessagePackSerializerOptions(IFormatterResolver resolver)
        : base(resolver) { }

    protected AllowStandardOrSerializableMessagePackSerializerOptions(
        MessagePackSerializerOptions copyFrom
    )
        : base(copyFrom) { }

    public override void ThrowIfDeserializingTypeIsDisallowed(Type type)
    {
        // Check against predefined blacklist
        base.ThrowIfDeserializingTypeIsDisallowed(type);

        // Check if MessagePack can handle this type safely
        var formatter = StandardResolver.Instance.GetFormatterDynamic(type);

        if (
            formatter == null
            && type.GetCustomAttributes(typeof(System.SerializableAttribute), true).Length == 0
        )
        {
            throw new MessagePackSerializationException(
                $"Deserialization attempted to create the type {type.FullName} which is not allowed." +
                $" Add 'System.Serializable' attribute to allow serialization"
            );
        }
    }

    protected override MessagePackSerializerOptions Clone()
    {
        return new AllowStandardOrSerializableMessagePackSerializerOptions(this);
    }
}
