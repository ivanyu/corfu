package corfu.logstorageunit;

import com.google.protobuf.MessageLite;
import corfu.logstorageunit.protocol.CommandParser;
import corfu.logstorageunit.unit.LogStorageUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientServingThread extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Socket socket;
    private final LogStorageUnit logStorageUnit;

    public ClientServingThread(final Socket socket, final LogStorageUnit logStorageUnit) {
        this.socket = socket;
        this.logStorageUnit = logStorageUnit;
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
}
