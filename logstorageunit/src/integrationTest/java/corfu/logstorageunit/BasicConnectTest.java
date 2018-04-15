package corfu.logstorageunit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class BasicConnectTest {

    private static LogStorageUnitServer server;
    private static Socket clientSocket;

    @BeforeClass
    public static void beforeClass() throws Exception {
        server = new LogStorageUnitServer(0);
        server.start();

        while (!server.isStarted()) {
            Thread.sleep(10);
        }
        clientSocket = new Socket();
        clientSocket.connect(new InetSocketAddress("localhost", server.getPort()));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        clientSocket.close();
        server.interrupt();
    }

    @Test
    public void connects() throws Exception {
        final OutputStream os = clientSocket.getOutputStream();
        os.write("HI\n".getBytes());
    }
}
