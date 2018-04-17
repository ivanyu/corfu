package corfu.logstorageunit;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

public class AcceptSealCommandTest extends WithServerConnection {
    private static final String INVALID_COMMAND = "invalid_command";

    @Test
    public void acceptsSeal() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream();
             final Scanner scanner = new Scanner(is)) {

            os.write("SEAL 42\n".getBytes());

            final String response = scanner.nextLine();
            Assert.assertNotEquals(INVALID_COMMAND, response);
        }
    }
}
