package corfu.server.projection;

import java.util.Objects;

public class LogRange extends Range {

    public LogRange(final long begin, final long end) {
        super(begin, end);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Range range = (Range) o;
        return begin == range.begin &&
                end == range.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(begin, end);
    }
}
