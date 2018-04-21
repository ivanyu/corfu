package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.ProtobufCommandResult;
import corfu.logstorageunit.protocol.SealCommand;
import corfu.logstorageunit.protocol.WriteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class WriteSealedTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToWriteSealed() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new SealCommand(1).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult sealCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.SEALED, sealCommandResult.getType());

            new WriteCommand(0, 1234, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult writeCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ERR_SEALED, writeCommandResult.getType());
            Assert.assertTrue(writeCommandResult.getContent().isEmpty());
        }
    }
}
