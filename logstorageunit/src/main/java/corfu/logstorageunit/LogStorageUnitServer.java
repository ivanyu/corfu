package corfu.logstorageunit;

import java.io.IOException;
import java.io.InputStream;
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
                     final Scanner scanner = new Scanner(inpStream)) {

                    System.out.println(scanner.nextLine());
                }

                socket.close();
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
