package corfu.storageunit;

import corfu.storageunit.Protocol.WriteCommandResult;
import corfu.storageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class WriteWrittenTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToWriteWritten() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {
            final byte[] pageToWrite = new byte[PAGE_SIZE];

            final Protocol.CommandWrapper writeCommand =
                    CommandFactory.createWriteCommand(0, 1234, pageToWrite);

            writeCommand.writeDelimitedTo(os);
            final WriteCommandResult commandResult1 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, commandResult1.getType());

            writeCommand.writeDelimitedTo(os);
            final WriteCommandResult commandResult2 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ERR_WRITTEN, commandResult2.getType());
        }
    }
}
