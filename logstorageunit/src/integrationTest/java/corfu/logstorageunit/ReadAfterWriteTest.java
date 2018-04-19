package corfu.logstorageunit;

import corfu.logstorageunit.protocol.WriteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

public class ReadAfterWriteCommandTest extends WithServerConnection {
    private static final String INVALID_COMMAND = "invalid_command";

    @Test(timeout = 300)
    public void acceptsWrite() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream();
             final Scanner scanner = new Scanner(is)) {

            new WriteCommand(42, 1234, "abc".getBytes()).toProtobuf()
                    .writeDelimitedTo(os);

            final String response = scanner.nextLine();
            Assert.assertNotEquals(INVALID_COMMAND, response);
        }
    }
}
