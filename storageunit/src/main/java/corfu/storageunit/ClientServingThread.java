package corfu.storageunit;

import com.google.protobuf.MessageLite;
import corfu.storageunit.protocol.CommandParser;
import corfu.storageunit.unit.StorageUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientServingThread extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Socket socket;
    private final StorageUnit storageUnit;

    public ClientServingThread(final Socket socket, final StorageUnit storageUnit) {
        this.socket = socket;
        this.storageUnit = storageUnit;
    }

    @Override
    public void run() {
        try (final InputStream inputStream = socket.getInputStream();
             final OutputStream outputStream = socket.getOutputStream()) {
            while (true) {
                final Protocol.CommandWrapper commandWrapper = CommandParser.parse(inputStream);
                if (commandWrapper == null) {
                    break;
                }
                logger.debug("Client sent command {}", commandWrapper);

                final MessageLite result = storageUnit.processCommand(commandWrapper);
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
}
