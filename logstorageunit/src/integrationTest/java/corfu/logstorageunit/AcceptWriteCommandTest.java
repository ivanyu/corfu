package corfu.logstorageunit;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

public class AcceptWriteCommandTest extends WithServerConnection {
    private static final String UNKNOWN_COMMAND = "unknown_command";

    @Test
    public void acceptsWrite() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream();
             final Scanner scanner = new Scanner(is)) {

            os.write("WRITE 42 1234 abc\n".getBytes());

            final String response = scanner.nextLine();
            Assert.assertNotEquals(UNKNOWN_COMMAND, response);
        }
    }
}
