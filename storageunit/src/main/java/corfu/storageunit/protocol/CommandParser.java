package corfu.storageunit.protocol;

import corfu.storageunit.Protocol.CommandWrapper;

import java.io.IOException;
import java.io.InputStream;

public class CommandParser {
    public static CommandWrapper parse(final InputStream inputStream) throws InvalidCommandException {
        try {
            return CommandWrapper.parseDelimitedFrom(inputStream);
        } catch (final IOException ex) {
            throw new InvalidCommandException("Error parsing command", ex);
        }
    }
}
