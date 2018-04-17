package corfu.logstorageunit;

import corfu.logstorageunit.command.Command;
import corfu.logstorageunit.command.CommandParser;
import corfu.logstorageunit.command.InvalidCommandException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

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

            while (true) {
                final Socket socket = ss.accept();

                try (final InputStream inpStream = socket.getInputStream();
                     final Scanner scanner = new Scanner(inpStream);
                     final OutputStream outputStream = socket.getOutputStream()) {

                    final String inputLine = scanner.nextLine();
                    System.out.println(inputLine);
                    try {
                        final Command command = CommandParser.parse(inputLine);
                        outputStream.write("ack".getBytes());
                        outputStream.flush();
                    } catch (final InvalidCommandException e) {
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
