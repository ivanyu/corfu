package corfu.server.projection;

public abstract class Range {
    protected final long begin;
    protected final long end;

    public Range(final long begin, final long end) {
        if (begin < 0) {
            throw new IllegalArgumentException("begin must be positive");
        }
        if (end < 0) {
            throw new IllegalArgumentException("end must be positive");
        }
        if (begin >= end) {
            throw new IllegalArgumentException("begin must be less than end");
        }

        this.begin = begin;
        this.end = end;
    }

    public long getBegin() {
        return begin;
    }

    public long getEnd() {
        return end;
    }

    public long getLength() {
        return end - begin;
    }
}
