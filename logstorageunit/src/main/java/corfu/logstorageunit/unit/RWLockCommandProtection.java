package corfu.logstorageunit.unit;

import java.util.concurrent.locks.Lock;

class LockCommandProtection implements CommandProtection {
    private volatile boolean finished = false;

    private final Lock lock1;
    private final Lock lock2;

    LockCommandProtection(final Lock lock1, final Lock lock2) {
        this.lock1 = lock1;
        this.lock2 = lock2;
    }

    @Override
    public void close() {
        if (finished) {
            throw new IllegalStateException("Can't finish more than once");
        }
        if (lock1 != null) {
            lock1.unlock();
        }
        if (lock2 != null) {
            lock2.unlock();
        }
        finished = true;
    }
}
