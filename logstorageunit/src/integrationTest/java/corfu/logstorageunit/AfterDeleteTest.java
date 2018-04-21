package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class AfterDeleteTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadAfterDelete() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            CommandFactory.createDeleteCommand(1234)
                    .writeDelimitedTo(os);
            final DeleteCommandResult deleteCommandResult =
                    DeleteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(DeleteCommandResult.Type.ACK, deleteCommandResult.getType());

            CommandFactory.createReadCommand(0, 1234)
                    .writeDelimitedTo(os);
            final ReadCommandResult readCommandResult =
                    ReadCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ReadCommandResult.Type.ERR_DELETED, readCommandResult.getType());
            Assert.assertTrue(readCommandResult.getContent().isEmpty());
        }
    }
    @Test(timeout = 300)
    public void notAllowToWriteAfterDelete() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            CommandFactory.createDeleteCommand(1234)
                    .writeDelimitedTo(os);
            final DeleteCommandResult deleteCommandResult =
                    DeleteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(DeleteCommandResult.Type.ACK, deleteCommandResult.getType());

            CommandFactory.createWriteCommand(0, 1234, "abc".getBytes())
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ERR_DELETED, writeCommandResult.getType());
        }
    }
}
