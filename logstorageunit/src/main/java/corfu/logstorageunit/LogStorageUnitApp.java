package corfu.logstorageunit;

import com.google.protobuf.InvalidProtocolBufferException;

public class LogStorageUnitApp {
    private static int PORT = 6666;

    public static void main(final String[] args) throws InterruptedException, InvalidProtocolBufferException {
        final LogStorageUnit logStorageUnit = new LogStorageUnit(4096, 10);

        final Thread serverThread = new LogStorageUnitServer(PORT, logStorageUnit);
        serverThread.start();
        serverThread.join();
    }
}
