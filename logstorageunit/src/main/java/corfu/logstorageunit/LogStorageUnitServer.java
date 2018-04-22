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
            final ServerSocket ss = new ServerSocket(this.port);
            this.actualPort = ss.getLocalPort();
            this.started = true;

            logger.info("Server started");

            while (true) {
                final Socket socket = ss.accept();
                logger.info("Client connected");
                serveClient(socket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void serveClient(final Socket socket) {
        try (final InputStream inputStream = socket.getInputStream();
             final OutputStream outputStream = socket.getOutputStream()) {
            while (true) {
                final CommandWrapper commandWrapper = CommandParser.parse(inputStream);
                if (commandWrapper == null) {
                    break;
                }
                logger.debug("Client sent command {}", commandWrapper);

                final MessageLite result = logStorageUnit.processCommand(commandWrapper);
                result.writeDelimitedTo(outputStream);
            }
        } catch (Exception e) {
            logger.warn("Exception while serving client", e);
        } finally {
            try {
                socket.close();
                logger.info("Client socket shutdown");
            } catch (IOException e) {
                logger.warn("Exception while closing socket", e);
            }
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
