package corfu.logstorageunit.command;

import java.util.Arrays;
import java.util.Objects;

class WriteCommand implements Command {
    private final int epoch;
    private final long address;
    private final byte[] content;

    WriteCommand(int epoch, long address, byte[] content) {
        this.epoch = epoch;
        this.address = address;
        this.content = content;
    }

    public int getEpoch() {
        return epoch;
    }

    public long getAddress() {
        return address;
    }

    public byte[] getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final WriteCommand that = (WriteCommand) o;
        return epoch == that.epoch &&
                address == that.address &&
                Arrays.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(epoch, address);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }
}
