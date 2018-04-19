package corfu.logstorageunit;

import corfu.logstorageunit.protocol.SealCommand;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

public class AcceptSealCommandTest extends WithServerConnection {
    private static final String INVALID_COMMAND = "invalid_command";

    @Ignore
    @Test(timeout = 300)
    public void acceptsSeal() throws Exception {
        try (final OutputStream os = clientSocket.getOutputStream();
             final InputStream is = clientSocket.getInputStream();
             final Scanner scanner = new Scanner(is)) {

            new SealCommand(42).toProtobuf()
                    .writeDelimitedTo(os);

            final String response = scanner.nextLine();
            Assert.assertNotEquals(INVALID_COMMAND, response);
        }
    }
}
