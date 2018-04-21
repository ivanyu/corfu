package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class ReadAfterWriteTest extends WithServerConnection {
    @Test(timeout = 300)
    public void allowToReadAfterWrite() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            CommandFactory.createWriteCommand(0, 1234, "abc".getBytes())
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult.getType());

            CommandFactory.createReadCommand(0, 1234)
                    .writeDelimitedTo(os);
            final ReadCommandResult readCommandResult =
                    ReadCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ReadCommandResult.Type.ACK, readCommandResult.getType());
            Assert.assertEquals("abc", readCommandResult.getContent().toStringUtf8());
        }
    }
}
