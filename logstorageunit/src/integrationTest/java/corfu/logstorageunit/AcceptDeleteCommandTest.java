package corfu.logstorageunit;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

public class AcceptDeleteCommandTest extends WithServerConnection {
    private static final String INVALID_COMMAND = "invalid_command";

    @Test
    public void acceptsDelete() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream();
             final Scanner scanner = new Scanner(is)) {

            os.write("DELETE 1234\n".getBytes());

            final String response = scanner.nextLine();
            Assert.assertNotEquals(INVALID_COMMAND, response);
        }
    }
}