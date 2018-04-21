package corfu.logstorageunit;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

class LogStorage {

    private int serverEpoch = 0;
    private HashMap<Long, Long> addressMap = new HashMap<>();
    private HashSet<Long> deletedAddresses = new HashSet<>();
    private ArrayList<byte[]> flash = new ArrayList<>();

    MessageLite processCommand(final Protocol.CommandWrapper commandWrapper) {
        switch (commandWrapper.getCommandCase()) {
            case READ:
                return processReadCommand(commandWrapper.getRead());

            case WRITE:
                return processWriteCommand(commandWrapper.getWrite());

            case DELETE:
                return processDeleteCommand(commandWrapper.getDelete());

            case SEAL:
                return processSealCommand(commandWrapper.getSeal());
        }
        assert false;
        return null;
    }

    private Protocol.ReadCommandResult processReadCommand(final Protocol.CommandWrapper.ReadCommand command) {
        if (serverEpoch > command.getEpoch()) {
            return Protocol.ReadCommandResult.newBuilder()
                    .setType(Protocol.ReadCommandResult.Type.ERR_SEALED)
                    .build();
        }

        if (deletedAddresses.contains(command.getAddress())) {
            return Protocol.ReadCommandResult.newBuilder()
                    .setType(Protocol.ReadCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (!addressMap.containsKey(command.getAddress())) {
            return Protocol.ReadCommandResult.newBuilder()
                    .setType(Protocol.ReadCommandResult.Type.ERR_UNWRITTEN)
                    .build();
        }

        final long physicalAddress = addressMap.get(command.getAddress());
        final byte[] content = flash.get((int) physicalAddress);

        return Protocol.ReadCommandResult.newBuilder()
                .setType(Protocol.ReadCommandResult.Type.ACK)
                .setContent(ByteString.copyFrom(content))
                .build();
    }

    private Protocol.WriteCommandResult processWriteCommand(final Protocol.CommandWrapper.WriteCommand command) {
        if (serverEpoch > command.getEpoch()) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_SEALED)
                    .build();
        }

        if (deletedAddresses.contains(command.getAddress())) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (addressMap.containsKey(command.getAddress())) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_WRITTEN)
                    .build();
        }

        addressMap.put(command.getAddress(), (long) flash.size());
        flash.add(command.getContent().toByteArray());

        return Protocol.WriteCommandResult.newBuilder()
                .setType(Protocol.WriteCommandResult.Type.ACK)
                .build();
    }

    private Protocol.DeleteCommandResult processDeleteCommand(final Protocol.CommandWrapper.DeleteCommand command) {
        deletedAddresses.add(command.getAddress());

        return Protocol.DeleteCommandResult.newBuilder()
                .setType(Protocol.DeleteCommandResult.Type.ACK)
                .build();
    }

    private Protocol.SealCommandResult processSealCommand(final Protocol.CommandWrapper.SealCommand command) {
        if (command.getEpoch() > serverEpoch) {
            serverEpoch = command.getEpoch();
            return Protocol.SealCommandResult.newBuilder()
                    .setType(Protocol.SealCommandResult.Type.ACK)
                    .setHighestAddress(getHighestAddress())
                    .build();
        } else {
            return Protocol.SealCommandResult.newBuilder()
                    .setType(Protocol.SealCommandResult.Type.ERR_SEALED)
                    .build();
        }
    }

    private long getHighestAddress() {
        if (addressMap.isEmpty()) {
            return -1;
        }
        // TODO optimise
        return Collections.max(addressMap.keySet());
    }
}
