package corfu.logstorageunit;

import com.google.protobuf.InvalidProtocolBufferException;

public class LogStorageApp {
    private static int PORT = 6666;

    public static void main(final String[] args) throws InterruptedException, InvalidProtocolBufferException {
        final Thread serverThread = new LogStorageServer(PORT);
        serverThread.start();
        serverThread.join();
    }
}
