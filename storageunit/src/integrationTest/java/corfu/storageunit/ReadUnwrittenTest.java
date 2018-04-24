package corfu.storageunit;

import corfu.storageunit.Protocol.ReadCommandResult;
import corfu.storageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class ReadUnwrittenTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadUnwritten() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            CommandFactory.createReadCommand(0, 1234)
                    .writeDelimitedTo(os);
            final ReadCommandResult commandResult =
                    ReadCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ReadCommandResult.Type.ERR_UNWRITTEN, commandResult.getType());
            Assert.assertTrue(commandResult.getContent().isEmpty());
        }
    }
}
