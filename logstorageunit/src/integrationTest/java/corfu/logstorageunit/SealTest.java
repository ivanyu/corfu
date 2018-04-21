package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.ReadCommand;
import corfu.logstorageunit.protocol.SealCommand;
import corfu.logstorageunit.protocol.WriteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

public class SealTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadSealed() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new WriteCommand(0, 0, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult1 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult1.getType());

            new WriteCommand(0, 1, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult2 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult2.getType());

            new WriteCommand(0, 3, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult3 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult3.getType());

            new SealCommand(1).toProtobuf()
                    .writeDelimitedTo(os);
            final SealCommandResult sealCommandResult =
                    SealCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(SealCommandResult.Type.ACK, sealCommandResult.getType());
            Assert.assertEquals(3, sealCommandResult.getHighestAddress());
        }
    }
}
