package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.ProtobufCommandResult;
import corfu.logstorageunit.protocol.ReadCommand;
import corfu.logstorageunit.protocol.WriteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class WriteWrittenTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToWriteWritten() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {
            final WriteCommand writeCommand = new WriteCommand(0, 1234, "abc".getBytes());

            writeCommand.toProtobuf().writeDelimitedTo(os);
            final ProtobufCommandResult commandResult1 =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ACK, commandResult1.getType());

            writeCommand.toProtobuf().writeDelimitedTo(os);
            final ProtobufCommandResult commandResult2 =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ERR_WRITTEN, commandResult2.getType());
            Assert.assertTrue(commandResult2.getContent().isEmpty());
        }
    }
}
