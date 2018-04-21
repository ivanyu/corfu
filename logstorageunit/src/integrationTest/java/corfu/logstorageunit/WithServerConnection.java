package corfu.logstorageunit;

import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.Socket;

class WithServerConnection {
    private LogStorageServer server;
    protected Socket clientSocket;

    @Before
    public void before() throws Exception {
        server = new LogStorageServer(0);
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
