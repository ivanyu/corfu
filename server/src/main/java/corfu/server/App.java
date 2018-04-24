package corfu.server;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import corfu.storageunit.Protocol.*;
import corfu.server.protocol.CommandFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class App {
    private static int PAGE_SIZE = 4096;

    public static void main(String[] args) throws IOException {
        final byte[] pageToWrite = createPageFromString("aaa");


        final Socket socket = new Socket();
        socket.connect(new InetSocketAddress("localhost", 7777));

        try (final OutputStream os = socket.getOutputStream();
             final InputStream is = socket.getInputStream()) {
            for (int i = 0; i < 4096; i++) {
                final CommandWrapper writeCommand =
                        CommandFactory.createWriteCommand(0, i, pageToWrite);
                writeCommand.writeDelimitedTo(os);

                final WriteCommandResult writeCommandResult =
                        WriteCommandResult.parseDelimitedFrom(is);
//                System.out.println(writeCommandResult.getType());

                final CommandWrapper readCommand =
                        CommandFactory.createReadCommand(0, i);
                readCommand.writeDelimitedTo(os);
                final ReadCommandResult readCommandResult =
                        ReadCommandResult.parseDelimitedFrom(is);
//                System.out.println(getString(readCommandResult.getContent()));
            }
        }
    }

    private static byte[] createPageFromString(final String string) {
        final byte[] strBytes = string.getBytes(Charsets.UTF_8);
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
