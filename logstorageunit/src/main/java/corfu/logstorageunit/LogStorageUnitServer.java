package corfu.logstorageunit;

import corfu.logstorageunit.protocol.Command;
import corfu.logstorageunit.protocol.CommandParser;
import corfu.logstorageunit.protocol.InvalidCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

class LogStorageUnitServer extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private volatile boolean started = false;
    private final int port;
    private volatile int actualPort = -1;

    LogStorageUnitServer(final int port) {
        this.port = port;
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

                try (final InputStream inputStream = socket.getInputStream();
                     final OutputStream outputStream = socket.getOutputStream()) {
                    try {
                        final Command command = CommandParser.parse(inputStream);
                        logger.debug("Client sent command {}", command);
                        outputStream.write("ack".getBytes());
                        outputStream.flush();
                    } catch (final InvalidCommandException e) {
                        logger.warn("", e);
                        outputStream.write("invalid_command".getBytes());
                        outputStream.flush();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        socket.close();
                        logger.info("Client socket shutdown");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

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
