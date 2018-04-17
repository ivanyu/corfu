package corfu.logstorageunit;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

public class NotAcceptUnknownCommandAndCloseConnectionTest extends WithServerConnection {
    private static final String INVALID_COMMAND = "invalid_command";

    @Test(timeout = 300)
    public void acceptsWrite() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream();
             final Scanner scanner = new Scanner(is)) {

            os.write("UNKNOWN 42 1234 abc\n".getBytes());

            final String response = scanner.nextLine();
            Assert.assertEquals(INVALID_COMMAND, response);

            do {
                Thread.sleep(10);
            } while (is.read() != -1);
        }
    }
}
