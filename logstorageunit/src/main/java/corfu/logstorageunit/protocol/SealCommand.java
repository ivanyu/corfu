package corfu.logstorageunit.protocol;

import corfu.logstorageunit.Protocol;

import java.util.Objects;

public final class SealCommand implements Command {
    private final int epoch;

    public SealCommand(int epoch) {
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

    @Override
    public Protocol.ProtobufCommand toProtobuf() {
        return Protocol.ProtobufCommand.newBuilder()
                .setType(Protocol.ProtobufCommand.Type.SEAL)
                .setEpoch(epoch)
                .build();
    }
}
