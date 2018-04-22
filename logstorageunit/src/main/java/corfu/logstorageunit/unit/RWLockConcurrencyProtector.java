package corfu.logstorageunit.unit;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RWLockConcurrencyProtector implements ConcurrencyProtector {

    private final int addressSpaceBucketsCount;
    private final ReadWriteLock[] addressSpaceBucketLocks;

    private final ReadWriteLock sealCommandRWLock = new ReentrantReadWriteLock();

    public RWLockConcurrencyProtector(final int addressSpaceBucketsCount) {
        assert addressSpaceBucketsCount > 0;

        this.addressSpaceBucketsCount = addressSpaceBucketsCount;
        this.addressSpaceBucketLocks = new ReadWriteLock[addressSpaceBucketsCount];
        for (int i = 0; i < addressSpaceBucketLocks.length; i++) {
            this.addressSpaceBucketLocks[i] = new ReentrantReadWriteLock();
        }
    }

    @Override
    public CommandProtection protectReadCommand(final long logicalPageNumber) {
        final Lock sealLock = sealCommandRWLock.readLock();
        sealLock.lock();

        final Lock pageLock = addressSpaceBucketLocks[getBucket(logicalPageNumber)].readLock();
        pageLock.lock();

        return new LockCommandProtection(pageLock, sealLock);
    }

    @Override
    public CommandProtection protectWriteCommand(final long logicalPageNumber) {
        final Lock sealLock = sealCommandRWLock.readLock();
        sealLock.lock();

        final Lock pageLock = addressSpaceBucketLocks[getBucket(logicalPageNumber)].writeLock();
        pageLock.lock();

        return new LockCommandProtection(pageLock, sealLock);
    }

    @Override
    public CommandProtection protectDeleteCommand(final long logicalPageNumber) {
        final Lock sealLock = sealCommandRWLock.readLock();
        sealLock.lock();

        final Lock pageLock = addressSpaceBucketLocks[getBucket(logicalPageNumber)].writeLock();
        pageLock.lock();

        return new LockCommandProtection(pageLock, sealLock);
    }

    @Override
    public CommandProtection protectSealCommand() {
        final Lock sealLock = sealCommandRWLock.writeLock();
        sealLock.lock();

        return new LockCommandProtection(null, sealLock);
    }

    private int getBucket(final long logicalPageNumber) {
        return (int) logicalPageNumber % addressSpaceBucketsCount;
    }
}
