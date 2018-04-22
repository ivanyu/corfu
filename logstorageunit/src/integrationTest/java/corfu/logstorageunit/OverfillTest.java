package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.WriteCommandResult;
import corfu.logstorageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class OverfillTest extends WithServerConnection {
    @Test(timeout = 2000)
    public void notAllowToOverfillStorage() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {
            final byte[] pageToWrite = new byte[PAGE_SIZE];

            for (int i = 0; i < PAGE_COUNT; i++) {
                final Protocol.CommandWrapper writeCommand =
                        CommandFactory.createWriteCommand(0, i, pageToWrite);
                writeCommand.writeDelimitedTo(os);

                final WriteCommandResult commandResult1 =
                        WriteCommandResult.parseDelimitedFrom(is);
                Assert.assertEquals(WriteCommandResult.Type.ACK, commandResult1.getType());
            }

            final Protocol.CommandWrapper writeCommand =
                    CommandFactory.createWriteCommand(0, PAGE_COUNT, pageToWrite);
            writeCommand.writeDelimitedTo(os);

            final WriteCommandResult commandResult2 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ERR_NO_FREE_PAGE, commandResult2.getType());
        }
    }
}
