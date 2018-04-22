package corfu.logstorageunit;

import corfu.logstorageunit.Protocol.*;
import corfu.logstorageunit.protocol.CommandFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class ReadAfterWriteTest extends WithServerConnection {
    @Test(timeout = 300)
    public void allowToReadAfterWrite() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            final byte[] pageContent = "abc".getBytes(Charset.forName("UTF-8"));
            final byte[] pageToWrite = new byte[PAGE_SIZE];
            System.arraycopy(pageContent, 0, pageToWrite, 0, pageContent.length);

            CommandFactory.createWriteCommand(0, 0, pageToWrite)
                    .writeDelimitedTo(os);
            final WriteCommandResult writeCommandResult =
                    WriteCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(WriteCommandResult.Type.ACK, writeCommandResult.getType());

            CommandFactory.createReadCommand(0, 0)
                    .writeDelimitedTo(os);
            final ReadCommandResult readCommandResult =
                    ReadCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ReadCommandResult.Type.ACK, readCommandResult.getType());
            Assert.assertArrayEquals(pageToWrite, readCommandResult.getContent().toByteArray());
        }
    }
}
