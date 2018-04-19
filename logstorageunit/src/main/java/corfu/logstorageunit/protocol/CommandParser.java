package corfu.logstorageunit.protocol;

import corfu.logstorageunit.Protocol;

import java.io.IOException;
import java.io.InputStream;

public class CommandParser {
    public static Command parse(final InputStream inputStream) throws InvalidCommandException {
        final Protocol.ProtobufCommand commandPB = parseProtobuf(inputStream);

        if (commandPB == null) {
            return null;
        }

        switch (commandPB.getType()) {
            case READ:
                return new ReadCommand(commandPB.getEpoch(), commandPB.getAddress());
            case WRITE:
                return new WriteCommand(
                        commandPB.getEpoch(), commandPB.getAddress(), commandPB.getContent().toByteArray());
            case DELETE:
                return new DeleteCommand(commandPB.getAddress());
            case SEAL:
                return new SealCommand(commandPB.getEpoch());
            default:
                throw new InvalidCommandException("Unknown command type", null);
        }
    }

    private static Protocol.ProtobufCommand parseProtobuf(final InputStream inputStream)
            throws InvalidCommandException {
        try {
            return Protocol.ProtobufCommand.parseDelimitedFrom(inputStream);
        } catch (final IOException ex) {
            throw new InvalidCommandException("Error parsing command", ex);
        }
    }
}
