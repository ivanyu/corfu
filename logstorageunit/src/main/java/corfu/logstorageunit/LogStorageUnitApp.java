package corfu.logstorageunit;

public class LogStorageUnitApp {
    private static int PORT = 6666;

    public static void main(final String[] args) throws InterruptedException {
        final Thread serverThread = new LogStorageUnitServer(PORT);
        serverThread.start();
        serverThread.join();
    }
}

/*
READ <epoch> <addr>
WRITE <epoch> <addr> <length> <bytes>
DELETE <addr>
SEAL <epoch>
 */