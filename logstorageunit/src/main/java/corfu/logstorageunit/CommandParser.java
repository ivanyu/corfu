package corfu.logstorageunit;

class CommandParser {
    private static UnknownCommand INVALID_COMMAND_EXCEPTION = new UnknownCommand();

    static Command parse(final String commandLine) throws UnknownCommand {
        final int firstSpace = commandLine.indexOf(' ');
        final String commandName = commandLine.substring(0, firstSpace);

        switch (commandName) {
            case "READ":
                return new ReadCommand();
            case "WRITE":
                return new WriteCommand();
            case "DELETE":
                return new DeleteCommand();
            case "SEAL":
                return new SealCommand();
            default:
                throw INVALID_COMMAND_EXCEPTION;
        }
    }
}
