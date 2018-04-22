package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class FreedPageUtilizationTest extends WithServerConnection {
    @Test(timeout = 2000)
    public void allowToWriteMoreThatPageCountAfterDelete() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {
            final byte[] pageToWrite = new byte[PAGE_SIZE];

            for (int i = 0; i < PAGE_COUNT; i++) {
                final CommandWrapper writeCommand =
                        CommandFactory.createWriteCommand(0, i, pageToWrite);
                writeCommand.writeDelimitedTo(os);

                final WriteCommandResult commandResult1 =
                        WriteCommandResult.parseDelimitedFrom(is);
                Assert.assertEquals(WriteCommandResult.Type.ACK, commandResult1.getType());
            }

            for (int i = 0; i < PAGE_COUNT; i++) {
                CommandFactory.createDeleteCommand(i)
                        .writeDelimitedTo(os);
                final DeleteCommandResult deleteCommandResult =
                        DeleteCommandResult.parseDelimitedFrom(is);
                Assert.assertEquals(DeleteCommandResult.Type.ACK, deleteCommandResult.getType());
            }

            for (int i = PAGE_COUNT; i < 2 * PAGE_COUNT; i++) {
                final CommandWrapper writeCommand =
                        CommandFactory.createWriteCommand(0, i, pageToWrite);
                writeCommand.writeDelimitedTo(os);

                final WriteCommandResult commandResult1 =
                        WriteCommandResult.parseDelimitedFrom(is);
                Assert.assertEquals(WriteCommandResult.Type.ACK, commandResult1.getType());
            }
        }
    }
}
