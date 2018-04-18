package corfu.logstorageunit.command;

import java.io.InputStream;
import java.util.Scanner;

public class CommandParser {
    private static InvalidCommandException INVALID_COMMAND_EXCEPTION = new InvalidCommandException();

    public static Command parse(final InputStream inputStream) throws InvalidCommandException {
        final Scanner scanner = new Scanner(inputStream);
        final String commandLine = scanner.nextLine();
        final String[] commandParts = getCommandLineParts(commandLine);

        if (commandParts.length < 1) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        final String commandName = commandParts[0];
        switch (commandName) {
            case "READ":
                return parseReadCommand(commandParts);
            case "WRITE":
                return null;
            case "DELETE":
                return parseDeleteCommand(commandParts);
            case "SEAL":
                return parseSealCommand(commandParts);
            default:
                throw INVALID_COMMAND_EXCEPTION;
        }
    }

    private static String[] getCommandLineParts(final String commandLine) {
        return commandLine.trim().split("\\s+");
    }

    private static ReadCommand parseReadCommand(final String[] commandParts) throws InvalidCommandException {
        if (commandParts.length != 3) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        int epoch = -1;
        try {
            epoch = Integer.parseInt(commandParts[1]);
        } catch (final NumberFormatException e) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        long address = -1;
        try {
            address = Long.parseLong(commandParts[2]);
        } catch (final NumberFormatException e) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        return new ReadCommand(epoch, address);
    }

    private static DeleteCommand parseDeleteCommand(final String[] commandParts) throws InvalidCommandException {
        if (commandParts.length != 2) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        long address = -1;
        try {
            address = Long.parseLong(commandParts[1]);
        } catch (final NumberFormatException e) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        return new DeleteCommand(address);
    }

    private static SealCommand parseSealCommand(final String[] commandParts) throws InvalidCommandException {
        if (commandParts.length != 2) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        int epoch = -1;
        try {
            epoch = Integer.parseInt(commandParts[1]);
        } catch (final NumberFormatException e) {
            throw INVALID_COMMAND_EXCEPTION;
        }

        return new SealCommand(epoch);
    }
}
