package corfu.logstorageunit;

import com.google.protobuf.MessageLite;
import corfu.logstorageunit.Protocol.CommandWrapper;
import corfu.logstorageunit.protocol.CommandParser;
import corfu.logstorageunit.unit.LogStorageUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

final class LogStorageUnitServer extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private volatile boolean started = false;
    private final int port;
    private volatile int actualPort = -1;

    private final LogStorageUnit logStorageUnit;

    LogStorageUnitServer(final int port,
                         final LogStorageUnit logStorageUnit) {
        this.port = port;
        this.logStorageUnit = logStorageUnit;
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
                new ClientServingThread(clientSocket, logStorageUnit).start();
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
