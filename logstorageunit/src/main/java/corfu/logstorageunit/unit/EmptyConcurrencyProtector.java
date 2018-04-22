package corfu.logstorageunit.unit;

public class EmptyConcurrencyProtector implements ConcurrencyProtector {
    private final CommandProtection EMPTY_PROTECTION = new EmptyCommandProtection();

    @Override
    public CommandProtection protectReadCommand(final long logicalPageNumber) {
        return EMPTY_PROTECTION;
    }

    @Override
    public CommandProtection protectWriteCommand(final long logicalPageNumber) {
        return EMPTY_PROTECTION;
    }

    @Override
    public CommandProtection protectDeleteCommand(final long logicalPageNumber) {
        return EMPTY_PROTECTION;
    }

    @Override
    public CommandProtection protectSealCommand() {
        return EMPTY_PROTECTION;
    }
}
