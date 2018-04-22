package corfu.logstorageunit;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import java.util.HashMap;
import java.util.HashSet;

class LogStorageUnit {

    private final int pageSize;
    private final int pageCount;
    private byte[] flash;

    private int serverEpoch = 0;
    private HashMap<Long, Integer> pageMap = new HashMap<>();
    private HashSet<Long> deletedPages = new HashSet<>();
    private long highestWrittenLogicalPageNumber = -1;
    private int highestWrittenPhysicalPageNumber = -1;

    LogStorageUnit(final int pageSize, final int pageCount) {
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        this.flash = new byte[pageSize * pageCount];
    }

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

        if (deletedPages.contains(command.getPageNumber())) {
            return Protocol.ReadCommandResult.newBuilder()
                    .setType(Protocol.ReadCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (!pageMap.containsKey(command.getPageNumber())) {
            return Protocol.ReadCommandResult.newBuilder()
                    .setType(Protocol.ReadCommandResult.Type.ERR_UNWRITTEN)
                    .build();
        }

        final int physicalPageNumber = pageMap.get(command.getPageNumber());
        final int physicalAddress = physicalPageNumber * pageSize;
        final ByteString content = ByteString.copyFrom(flash, physicalAddress, pageSize);

        return Protocol.ReadCommandResult.newBuilder()
                .setType(Protocol.ReadCommandResult.Type.ACK)
                .setContent(content)
                .build();
    }

    private Protocol.WriteCommandResult processWriteCommand(final Protocol.CommandWrapper.WriteCommand command) {
        if (command.getContent().size() != pageSize) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_CONTENT_SIZE)
                    .build();
        }

        if (serverEpoch > command.getEpoch()) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_SEALED)
                    .build();
        }

        if (deletedPages.contains(command.getPageNumber())) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (pageMap.containsKey(command.getPageNumber())) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_WRITTEN)
                    .build();
        }

        if (highestWrittenPhysicalPageNumber + 1 >= pageCount) {
            return Protocol.WriteCommandResult.newBuilder()
                    .setType(Protocol.WriteCommandResult.Type.ERR_NO_FREE_PAGE)
                    .build();
        }

        highestWrittenLogicalPageNumber = Math.max(
                highestWrittenLogicalPageNumber, command.getPageNumber());

        highestWrittenPhysicalPageNumber += 1;
        final int newPhysicalAddress = highestWrittenPhysicalPageNumber * pageSize;
        pageMap.put(command.getPageNumber(), newPhysicalAddress);

        command.getContent().copyTo(flash, newPhysicalAddress);

        return Protocol.WriteCommandResult.newBuilder()
                .setType(Protocol.WriteCommandResult.Type.ACK)
                .build();
    }

    private Protocol.DeleteCommandResult processDeleteCommand(final Protocol.CommandWrapper.DeleteCommand command) {
        deletedPages.add(command.getAddress());

        return Protocol.DeleteCommandResult.newBuilder()
                .setType(Protocol.DeleteCommandResult.Type.ACK)
                .build();
    }

    private Protocol.SealCommandResult processSealCommand(final Protocol.CommandWrapper.SealCommand command) {
        if (command.getEpoch() > serverEpoch) {
            serverEpoch = command.getEpoch();
            return Protocol.SealCommandResult.newBuilder()
                    .setType(Protocol.SealCommandResult.Type.ACK)
                    .setHighestPageNumber(highestWrittenLogicalPageNumber)
                    .build();
        } else {
            return Protocol.SealCommandResult.newBuilder()
                    .setType(Protocol.SealCommandResult.Type.ERR_SEALED)
                    .build();
        }
    }
}
