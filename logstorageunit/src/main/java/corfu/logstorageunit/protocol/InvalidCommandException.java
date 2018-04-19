package corfu.logstorageunit.protocol;

public final class InvalidCommandException extends Exception {
    public InvalidCommandException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
