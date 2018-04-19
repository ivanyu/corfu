package corfu.logstorageunit;

import corfu.logstorageunit.protocol.Command;
import corfu.logstorageunit.protocol.CommandParser;
import corfu.logstorageunit.protocol.InvalidCommandException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

class LogStorageUnitServer extends Thread {
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

            System.out.println("Server started");

            while (true) {
                final Socket socket = ss.accept();
                System.out.println("Client connected");

                try (final InputStream inputStream = socket.getInputStream();
                     final OutputStream outputStream = socket.getOutputStream()) {
                    try {
                        final Command command = CommandParser.parse(inputStream);
                        outputStream.write("ack".getBytes());
                        outputStream.flush();
                    } catch (final InvalidCommandException e) {
                        e.printStackTrace();
                        outputStream.write("invalid_command".getBytes());
                        outputStream.flush();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        socket.close();
                        System.out.println("Socket shut down");
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
