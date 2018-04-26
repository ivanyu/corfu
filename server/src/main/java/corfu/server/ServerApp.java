package corfu.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

import corfu.server.projection.LogRange;
import corfu.server.projection.Projection;
import corfu.server.projection.StorageUnitPage;
import corfu.server.projection.StorageUnitRange;
import corfu.server.protocol.CommandFactory;
import corfu.storageunit.Protocol.CommandWrapper;
import corfu.storageunit.Protocol.ReadCommandResult;
import corfu.storageunit.Protocol.WriteCommandResult;

public class ServerApp {
    private static int PAGE_SIZE = 4096;
    private static int LOG_LENGTH = 256;

    public static void main(String[] args) throws IOException {
        final Map<String, String> storageUnitAddrs = new HashMap<String, String>() {{
            put("unit1", "localhost:7771");
            put("unit2", "localhost:7772");
        }};

        final Projection projection = new Projection(
                new LogRange(0, LOG_LENGTH),
                Arrays.asList(
                        new StorageUnitRange(0, 128, "unit1"),
                        new StorageUnitRange(0, 128, "unit2")
                )
        );

        final Map<String, Socket> storageUnitSockets = connect(storageUnitAddrs);

        for (int logPosition = 0; logPosition < LOG_LENGTH; logPosition++) {
            final byte[] pageToWrite = createPageFromString("aaa");

            final StorageUnitPage storageUnitPage = projection.project(logPosition);
            final Socket socket = storageUnitSockets.get(storageUnitPage.getStorageUnitId());

            final CommandWrapper writeCommand =
                    CommandFactory.createWriteCommand(0, storageUnitPage.getPageNumber(), pageToWrite);
            writeCommand.writeDelimitedTo(socket.getOutputStream());

            final WriteCommandResult writeCommandResult =
                    WriteCommandResult.parseDelimitedFrom(socket.getInputStream());
            System.out.println(writeCommandResult.getType());

            final CommandWrapper readCommand =
                    CommandFactory.createReadCommand(0, storageUnitPage.getPageNumber());
            readCommand.writeDelimitedTo(socket.getOutputStream());
            final ReadCommandResult readCommandResult =
                    ReadCommandResult.parseDelimitedFrom(socket.getInputStream());
//                    System.out.println(getString(readCommandResult.getContent()));
        }

        disconnect(storageUnitSockets);
    }

    private static Map<String, Socket> connect(final Map<String, String> storageUnitAddrs) throws IOException {
        final Map<String, Socket> result = new HashMap<>();
        for (Map.Entry<String, String> entry : storageUnitAddrs.entrySet()) {
            final String storageUnitId = entry.getKey();
            final String addressAndPort = entry.getValue();
            result.put(storageUnitId, connect(addressAndPort));
        }
        return result;
    }

    private static Socket connect(final String addressAndPort) throws IOException {
        final String[] parts = addressAndPort.split(":");
        assert parts.length == 2;

        final String host = parts[0];
        final int port = Integer.parseInt(parts[1]);
        final Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    private static void disconnect(final Map<String, Socket> storageUnitSockets) throws IOException {
        for (final Socket s : storageUnitSockets.values()) {
            s.close();
        }
    }

    private static byte[] createPageFromString(final String value) {
        final byte[] strBytes = value.getBytes(Charsets.UTF_8);
        return ByteBuffer.allocate(PAGE_SIZE)
                .putInt(strBytes.length)
                .put(strBytes)
                .array();
    }

    private static String getString(final ByteString pageContent) {
        final ByteBuffer buf = pageContent.asReadOnlyByteBuffer();
        final int len = buf.getInt();
        final byte[] bytes = new byte[len];
        buf.get(bytes, 0, len);
        return new String(bytes, Charsets.UTF_8);
    }
}
