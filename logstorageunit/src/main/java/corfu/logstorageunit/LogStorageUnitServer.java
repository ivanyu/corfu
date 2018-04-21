package corfu.logstorageunit;

import com.google.protobuf.ByteString;
import corfu.logstorageunit.protocol.*;
import corfu.logstorageunit.Protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

final class LogStorageUnitServer extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private volatile boolean started = false;
    private final int port;
    private volatile int actualPort = -1;

    LogStorageUnitServer(final int port) {
        this.port = port;
    }

    private int serverEpoch = 0;
    private HashMap<Long, Long> addressMap = new HashMap<>();
    private HashSet<Long> deletedAddresses = new HashSet<>();
    private ArrayList<byte[]> flash = new ArrayList<>();

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
            try {
                while (true) {
                    final Command command = CommandParser.parse(inputStream);
                    if (command == null) {
                        break;
                    }

                    logger.debug("Client sent command {}", command);

                    final ProtobufCommandResult commandResult = processCommand(command);
                    commandResult.writeDelimitedTo(outputStream);
                }
            } catch (final InvalidCommandException e) {
                logger.warn("", e);
                ProtobufCommandResult.newBuilder()
                        .setType(Protocol.ProtobufCommandResult.Type.INVALID_COMMAND)
                        .build()
                        .writeDelimitedTo(outputStream);
            }
        } catch (Exception e) {
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

    int getPort() {
        if (!this.started) {
            throw new IllegalStateException("Not started");
        }
        return actualPort;
    }

    boolean isStarted() {
        return this.started;
    }

    private ProtobufCommandResult processCommand(final Command command) {
        if (command instanceof WriteCommand) {
            return processWriteCommand((WriteCommand) command);
        } else if (command instanceof ReadCommand) {
            return processReadCommand((ReadCommand) command);
        } else if (command instanceof DeleteCommand) {
            return processDeleteCommand((DeleteCommand) command);
        } else if (command instanceof SealCommand) {
            return processSealCommand((SealCommand) command);
        }
        assert false;
        return null;
    }

    private ProtobufCommandResult processWriteCommand(final WriteCommand command) {
        if (serverEpoch > command.getEpoch()) {
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.ERR_SEALED)
                    .build();
        }

        if (deletedAddresses.contains(command.getAddress())) {
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (addressMap.containsKey(command.getAddress())) {
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.ERR_WRITTEN)
                    .build();
        }

        addressMap.put(command.getAddress(), (long) flash.size());
        flash.add(command.getContent());

        return ProtobufCommandResult.newBuilder()
                .setType(ProtobufCommandResult.Type.ACK)
                .build();
    }

    private ProtobufCommandResult processReadCommand(final ReadCommand command) {
        if (serverEpoch > command.getEpoch()) {
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.ERR_SEALED)
                    .build();
        }

        if (deletedAddresses.contains(command.getAddress())) {
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (!addressMap.containsKey(command.getAddress())) {
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.ERR_UNWRITTEN)
                    .build();
        }

        final long physicalAddress = addressMap.get(command.getAddress());
        final byte[] content = flash.get((int) physicalAddress);

        return ProtobufCommandResult.newBuilder()
                .setType(ProtobufCommandResult.Type.ACK)
                .setContent(ByteString.copyFrom(content))
                .build();
    }

    private ProtobufCommandResult processDeleteCommand(final DeleteCommand command) {
        deletedAddresses.add(command.getAddress());

        return ProtobufCommandResult.newBuilder()
                .setType(ProtobufCommandResult.Type.ACK)
                .build();
    }

    private ProtobufCommandResult processSealCommand(final SealCommand command) {
        if (command.getEpoch() > serverEpoch) {
            serverEpoch = command.getEpoch();
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.SEALED)
                    .setHighestAddress(getHighestAddress())
                    .build();
        } else {
            return ProtobufCommandResult.newBuilder()
                    .setType(ProtobufCommandResult.Type.ERR_SEALED)
                    .build();
        }
    }

    private long getHighestAddress() {
        if (addressMap.isEmpty()) {
            return -1;
        }
        // TODO optimise
        return Collections.max(addressMap.keySet());
    }
}
