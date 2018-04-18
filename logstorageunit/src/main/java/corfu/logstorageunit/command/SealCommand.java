package corfu.logstorageunit.command;

import java.util.Objects;

class SealCommand implements Command {
    private final int epoch;

    SealCommand(int epoch) {
        this.epoch = epoch;
    }

    public int getEpoch() {
        return epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SealCommand that = (SealCommand) o;
        return epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch);
    }
}
