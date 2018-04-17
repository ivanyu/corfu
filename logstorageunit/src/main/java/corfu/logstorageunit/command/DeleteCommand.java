package corfu.logstorageunit.command;

import java.util.Objects;

class DeleteCommand implements Command {
    private final long address;

    DeleteCommand(long address) {
        this.address = address;
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

        final DeleteCommand that = (DeleteCommand) o;
        return address == that.address;
    }

    @Override
    public int hashCode() {
        return Objects.hash(address);
    }
}
