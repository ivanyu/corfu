package corfu.logstorageunit.unit;

public interface ConcurrencyProtector {
    CommandProtection protectReadCommand(final long logicalPageNumber);
    CommandProtection protectWriteCommand(final long logicalPageNumber);
    CommandProtection protectDeleteCommand(final long logicalPageNumber);
    CommandProtection protectSealCommand();
}
