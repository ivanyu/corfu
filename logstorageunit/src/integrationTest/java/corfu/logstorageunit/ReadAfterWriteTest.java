package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.ReadCommand;
import corfu.logstorageunit.protocol.WriteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class ReadAfterWriteTest extends WithServerConnection {
    @Test(timeout = 300)
    public void allowToReadAfterWrite() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new WriteCommand(0, 1234, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult.getType());

            new ReadCommand(0, 1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ReadCommandResult readCommandResult =
                    ReadCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ReadCommandResult.Type.ACK, readCommandResult.getType());
            Assert.assertEquals("abc", readCommandResult.getContent().toStringUtf8());
        }
    }
}
