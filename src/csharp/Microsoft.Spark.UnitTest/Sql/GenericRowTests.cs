// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.UnitTest.TestUtils;
using Microsoft.Spark.Utils;
using Moq;
using Razorvine.Pickle;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class GenericRowTests
    {        
        [Fact]
        public void GenericRowTest()
        {            
            var row = new GenericRow(new object[] { 1, "abc" });

            // Validate Size().
            Assert.Equal(2, row.Size());

            // Validate [] operator.
            Assert.Equal(1, row[0]);
            Assert.Equal("abc", row[1]);

            // Validate Get*(int).
            Assert.Equal(1, row.Get(0));
            Assert.Equal("abc", row.Get(1));
            Assert.Equal(1, row.GetAs<int>(0));
            Assert.ThrowsAny<Exception>(() => row.GetAs<string>(0));
            Assert.Equal("abc", row.GetAs<string>(1));
            Assert.ThrowsAny<Exception>(() => row.GetAs<int>(1));            
        }

        [Fact]
        public void GenericRowConstructorTest()
        {
            Pickler pickler = CreatePickler();            
            var row1 = new GenericRow(new object[] { 10, "name1" });
            var row2 = new GenericRow(new object[] { 15, "name2" });
            var pickledBytes = pickler.dumps(new[] { row1, row2 });

            // Note that the following will invoke RowConstructor.construct().
            var unpickledData = PythonSerDe.GetUnpickledObjects(
                new MemoryStream(pickledBytes),
                pickledBytes.Length);

            Assert.Equal(2, unpickledData.Length);
            //Assert.Equal(row1, (unpickledData[0] as RowConstructor).GetRow());
            //Assert.Equal(row2, (unpickledData[1] as RowConstructor).GetRow());
        }

        [Fact]
        public void RowCollectorTest()
        {
            var stream = new MemoryStream();
            Pickler pickler = CreatePickler();

            // Pickle two rows in one batch.
            var row1 = new GenericRow(new object[] { 10, "name1" });
            var row2 = new GenericRow(new object[] { 15, "name2" });
            var batch1 = pickler.dumps(new[] { row1, row2 });
            SerDe.Write(stream, batch1.Length);
            SerDe.Write(stream, batch1);

            // Pickle one row in one batch.
            var row3 = new GenericRow(new object[] { 20, "name3" });
            var batch2 = pickler.dumps(new[] { row3 });
            SerDe.Write(stream, batch2.Length);
            SerDe.Write(stream, batch2);

            // Rewind the memory stream so that the row collect can read from beginning.
            stream.Seek(0, SeekOrigin.Begin);

            // Set up the mock to return memory stream to which pickled data is written.
            var socket = new Mock<ISocketWrapper>();
            socket.Setup(m => m.InputStream).Returns(stream);
            socket.Setup(m => m.OutputStream).Returns(stream);

            var rowCollector = new RowCollector();
            Row[] rows = rowCollector.Collect(socket.Object).ToArray();

            Assert.Equal(3, rows.Length);
            //Assert.Equal(row1, rows[0]);
            //Assert.Equal(row2, rows[1]);
            //Assert.Equal(row3, rows[2]);
        }

        private Pickler CreatePickler()
        {
            new StructTypePickler().Register();
            new RowPickler().Register();
            return new Pickler();
        }
    }
}
