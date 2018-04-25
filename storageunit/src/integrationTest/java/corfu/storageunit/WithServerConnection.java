package corfu.storageunit;

import com.codahale.metrics.MetricRegistry;
import corfu.storageunit.unit.ConcurrencyProtector;
import corfu.storageunit.unit.StorageUnit;
import corfu.storageunit.unit.EmptyConcurrencyProtector;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.Socket;

class WithServerConnection {
    private StorageUnitServer server;
    protected Socket clientSocket;

    protected int PAGE_SIZE = 10;
    protected int PAGE_COUNT = 10;

    @Before
    public void before() throws Exception {
        final ConcurrencyProtector lockMechanism = new EmptyConcurrencyProtector();
        final MetricRegistry metricRegistry = new MetricRegistry();
        final StorageUnit storageUnit =
                new StorageUnit(PAGE_SIZE, PAGE_COUNT, lockMechanism, metricRegistry);
        server = new StorageUnitServer(0, storageUnit);
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
