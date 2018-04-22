package corfu.logstorageunit;

import corfu.logstorageunit.unit.ConcurrencyProtector;
import corfu.logstorageunit.unit.LogStorageUnit;
import corfu.logstorageunit.unit.EmptyConcurrencyProtector;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.Socket;

class WithServerConnection {
    private LogStorageUnitServer server;
    protected Socket clientSocket;

    protected int PAGE_SIZE = 10;
    protected int PAGE_COUNT = 10;

    @Before
    public void before() throws Exception {
        final ConcurrencyProtector lockMechanism = new EmptyConcurrencyProtector();
        final LogStorageUnit logStorageUnit =
                new LogStorageUnit(PAGE_SIZE, PAGE_COUNT, lockMechanism);
        server = new LogStorageUnitServer(0, logStorageUnit);
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
