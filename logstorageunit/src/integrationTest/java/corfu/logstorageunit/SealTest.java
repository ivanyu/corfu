package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class SealTest extends WithServerConnection {
    @Test(timeout = 300)
    public void notAllowToReadSealed() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            CommandFactory.createWriteCommand(0, 0, "abc".getBytes(Charset.forName("UTF-8")))
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult1 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult1.getType());

            CommandFactory.createWriteCommand(0, 1, "abc".getBytes(Charset.forName("UTF-8")))
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult2 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult2.getType());

            CommandFactory.createWriteCommand(0, 3, "abc".getBytes(Charset.forName("UTF-8")))
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult3 =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult3.getType());

            CommandFactory.createSealCommand(1)
                    .writeDelimitedTo(os);
            final SealCommandResult sealCommandResult =
                    SealCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(SealCommandResult.Type.ACK, sealCommandResult.getType());
            Assert.assertEquals(3, sealCommandResult.getHighestAddress());
        }
    }
}
