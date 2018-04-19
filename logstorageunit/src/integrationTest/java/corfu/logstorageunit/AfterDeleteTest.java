package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.ProtobufCommandResult;
import corfu.logstorageunit.protocol.DeleteCommand;
import corfu.logstorageunit.protocol.ReadCommand;
import corfu.logstorageunit.protocol.WriteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class ReadAfterDeleteTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadAfterDelete() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new DeleteCommand(1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult writeCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ACK, writeCommandResult.getType());

            new ReadCommand(0, 1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult readCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ACK, readCommandResult.getType());
            Assert.assertEquals("abc", readCommandResult.getContent().toStringUtf8());
        }
    }
}
