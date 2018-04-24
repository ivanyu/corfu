package corfu.storageunit;

import corfu.storageunit.Protocol.WriteCommandResult;
import corfu.storageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class WriteIncorrectPageSizeTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToWritePageWithIncorrectSize() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            final Protocol.CommandWrapper writeCommand =
                    CommandFactory.createWriteCommand(0, 0, new byte[1]);
            writeCommand.writeDelimitedTo(os);

            final WriteCommandResult commandResult =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ERR_CONTENT_SIZE, commandResult.getType());
            }
    }
}
