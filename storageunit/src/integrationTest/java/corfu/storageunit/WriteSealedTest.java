package corfu.storageunit;

import corfu.storageunit.Protocol.*;
import corfu.storageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class WriteSealedTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToWriteSealed() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            final byte[] pageToWrite = new byte[PAGE_SIZE];

            CommandFactory.createSealCommand(1)
                    .writeDelimitedTo(os);
            final SealCommandResult sealCommandResult =
                    SealCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(SealCommandResult.Type.ACK, sealCommandResult.getType());

            CommandFactory.createWriteCommand(0, 1234, pageToWrite)
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ERR_SEALED, writeCommandResult.getType());
        }
    }
}
