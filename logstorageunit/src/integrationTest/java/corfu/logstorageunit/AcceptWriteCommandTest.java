package corfu.logstorageunit;

import corfu.logstorageunit.protocol.WriteCommand;
import corfu.logstorageunit.Protocol.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

public class AcceptWriteCommandTest extends WithServerConnection {
    private static final String INVALID_COMMAND = "invalid_command";

    @Ignore
    @Test(timeout = 300)
    public void acceptsWrite() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream()) {

            new WriteCommand(42, 1234, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);

            final ProtobufCommandResult writeCommandResult =
                    ProtobufCommandResult.parseDelimitedFrom(is);
            Assert.assertEquals(ProtobufCommandResult.Type.ACK, writeCommandResult.getType());
        }
    }
}
