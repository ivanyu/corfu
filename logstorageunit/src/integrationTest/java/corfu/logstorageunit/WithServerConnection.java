package corfu.logstorageunit;

import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.Socket;

class WithServerConnection {
    private LogStorageUnitServer server;
    protected Socket clientSocket;

    @Before
    public void before() throws Exception {
        server = new LogStorageUnitServer(0);
        server.start();

        while (!server.isStarted()) {
            Thread.sleep(10);
        }
        clientSocket = new Socket();
        clientSocket.connect(new InetSocketAddress("localhost", server.getPort()));
    }

    @After
    public void after() throws Exception {
        clientSocket.close();
        server.interrupt();
    }
}
