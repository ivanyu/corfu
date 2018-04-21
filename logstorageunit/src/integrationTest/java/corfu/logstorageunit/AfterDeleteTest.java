package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.ProtobufCommandResult;
import corfu.logstorageunit.protocol.DeleteCommand;
import corfu.logstorageunit.protocol.ReadCommand;
import corfu.logstorageunit.protocol.WriteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class AfterDeleteTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadAfterDelete() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new DeleteCommand(1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult deleteCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ACK, deleteCommandResult.getType());

            new ReadCommand(0, 1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult readCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ERR_DELETED, readCommandResult.getType());
            Assert.assertTrue(readCommandResult.getContent().isEmpty());
        }
    }
    @Test(timeout = 300)
    public void notAllowToWriteAfterDelete() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new DeleteCommand(1234).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult deleteCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ACK, deleteCommandResult.getType());

            new WriteCommand(0, 1234, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);
            final ProtobufCommandResult writeCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ERR_DELETED, writeCommandResult.getType());
            Assert.assertTrue(writeCommandResult.getContent().isEmpty());
        }
    }
}
