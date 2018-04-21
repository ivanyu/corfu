package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class WriteSealedTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToWriteSealed() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            CommandFactory.createSealCommand(1)
                    .writeDelimitedTo(os);
            final SealCommandResult sealCommandResult =
                    SealCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(SealCommandResult.Type.ACK, sealCommandResult.getType());

            CommandFactory.createWriteCommand(0, 1234, "abc".getBytes())
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ERR_SEALED, writeCommandResult.getType());
        }
    }
}
