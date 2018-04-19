package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.ProtobufCommandResult;
import corfu.logstorageunit.protocol.ReadCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class ReadUnwrittenTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadUnwritten() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {
            new ReadCommand(0, 1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult commandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ERR_UNWRITTEN, commandResult.getType());
            Assert.assertTrue(commandResult.getContent().isEmpty());
        }
    }
}
