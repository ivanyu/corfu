package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.ReadCommand;
import corfu.logstorageunit.protocol.SealCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class ReadSealedTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadSealed() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new SealCommand(1).toProtobuf()
                    .writeDelimitedTo(os);
            final SealCommandResult sealCommandResult =
                    SealCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(SealCommandResult.Type.ACK, sealCommandResult.getType());

            new ReadCommand(0, 1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ReadCommandResult readCommandResult =
                    ReadCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ReadCommandResult.Type.ERR_SEALED, readCommandResult.getType());
            Assert.assertTrue(readCommandResult.getContent().isEmpty());
        }
    }
}
