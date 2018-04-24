package corfu.storageunit;

import corfu.storageunit.unit.StorageUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

final class StorageUnitServer extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private volatile boolean started = false;
    private final int port;
    private volatile int actualPort = -1;

    private final StorageUnit storageUnit;

    StorageUnitServer(final int port,
                      final StorageUnit storageUnit) {
        this.port = port;
        this.storageUnit = storageUnit;
    }

    @Override
    public void run() {
        try {
            final ServerSocket serverSocket = new ServerSocket(this.port);
            this.actualPort = serverSocket.getLocalPort();
            this.started = true;

            logger.info("Server started");

            while (true) {
                final Socket clientSocket = serverSocket.accept();
                logger.info("Client connected");
                new ClientServingThread(clientSocket, storageUnit).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    int getPort() {
        if (!this.started) {
            throw new IllegalStateException("Not started");
        }
        return actualPort;
    }

    boolean isStarted() {
        return this.started;
    }
}
