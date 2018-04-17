package corfu.logstorageunit.command;

import java.util.Objects;

final class ReadCommand implements Command {
    private final int epoch;
    private final long address;

    ReadCommand(int epoch, long address) {
        this.epoch = epoch;
        this.address = address;
    }

    public int getEpoch() {
        return epoch;
    }

    public long getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ReadCommand that = (ReadCommand) o;
        return epoch == that.epoch &&
                address == that.address;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, address);
    }
}
