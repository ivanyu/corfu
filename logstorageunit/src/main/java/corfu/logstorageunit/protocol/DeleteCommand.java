package corfu.logstorageunit.protocol;

import corfu.logstorageunit.Protocol;

import java.util.Objects;

public final class DeleteCommand implements Command {
    private final long address;

    public DeleteCommand(long address) {
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

    public Protocol.ProtobufCommand toProtobuf() {
        return Protocol.ProtobufCommand.newBuilder()
                .setType(Protocol.ProtobufCommand.Type.DELETE)
                .setAddress(address)
                .build();
    }

    @Override
    public String toString() {
        return "DeleteCommand(address=" + address + ")";
    }
}
