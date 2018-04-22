package corfu.logstorageunit;

import com.google.protobuf.InvalidProtocolBufferException;
import corfu.logstorageunit.unit.ConcurrencyProtector;
import corfu.logstorageunit.unit.LogStorageUnit;
import corfu.logstorageunit.unit.RWLockConcurrencyProtector;

public class LogStorageUnitApp {
    private static int PORT = 6666;

    public static void main(final String[] args) throws InterruptedException, InvalidProtocolBufferException {
        final ConcurrencyProtector lockMechanism = new RWLockConcurrencyProtector(16);
        final LogStorageUnit logStorageUnit = new LogStorageUnit(
                4096, 10, lockMechanism);

        final Thread serverThread = new LogStorageUnitServer(PORT, logStorageUnit);
        serverThread.start();
        serverThread.join();
    }
}
