package corfu.logstorageunit.protocol;

import corfu.logstorageunit.Protocol;

import java.util.Objects;

final public class ReadCommand implements Command {
    private final int epoch;
    private final long address;

    public ReadCommand(int epoch, long address) {
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

    @Override
    public Protocol.ProtobufCommand toProtobuf() {
        return Protocol.ProtobufCommand.newBuilder()
                .setType(Protocol.ProtobufCommand.Type.READ)
                .setEpoch(epoch)
                .setAddress(address)
                .build();
    }
}
