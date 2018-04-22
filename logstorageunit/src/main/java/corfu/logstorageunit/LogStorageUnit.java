package corfu.logstorageunit;

import java.util.HashMap;
import java.util.HashSet;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import corfu.logstorageunit.Protocol.*;

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

    MessageLite processCommand(final CommandWrapper commandWrapper) {
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

    private ReadCommandResult processReadCommand(final CommandWrapper.ReadCommand command) {
        if (isPastEpoch(command.getEpoch())) {
            return ReadCommandResult.newBuilder()
                    .setType(ReadCommandResult.Type.ERR_SEALED)
                    .build();
        }

        if (isPageDeleted(command.getPageNumber())) {
            return ReadCommandResult.newBuilder()
                    .setType(ReadCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (!isPageWritten(command.getPageNumber())) {
            return ReadCommandResult.newBuilder()
                    .setType(ReadCommandResult.Type.ERR_UNWRITTEN)
                    .build();
        }

        final int physicalPageNumber = pageMap.get(command.getPageNumber());
        final int physicalAddress = physicalPageNumber * pageSize;
        final ByteString content = ByteString.copyFrom(flash, physicalAddress, pageSize);

        return ReadCommandResult.newBuilder()
                .setType(ReadCommandResult.Type.ACK)
                .setContent(content)
                .build();
    }

    private WriteCommandResult processWriteCommand(final CommandWrapper.WriteCommand command) {
        if (!isCorrectContentSize(command)) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_CONTENT_SIZE)
                    .build();
        }

        if (isPastEpoch(command.getEpoch())) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_SEALED)
                    .build();
        }

        if (isPageDeleted(command.getPageNumber())) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (isPageWritten(command.getPageNumber())) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_WRITTEN)
                    .build();
        }

        if (canWriteOneMorePage()) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_NO_FREE_PAGE)
                    .build();
        }

        highestWrittenLogicalPageNumber = Math.max(
                highestWrittenLogicalPageNumber, command.getPageNumber());

        highestWrittenPhysicalPageNumber += 1;
        final int newPhysicalAddress = highestWrittenPhysicalPageNumber * pageSize;
        pageMap.put(command.getPageNumber(), newPhysicalAddress);

        command.getContent().copyTo(flash, newPhysicalAddress);

        return WriteCommandResult.newBuilder()
                .setType(WriteCommandResult.Type.ACK)
                .build();
    }

    private boolean isPastEpoch(final int commandEpoch) {
        return commandEpoch < serverEpoch;
    }

    private boolean isCorrectContentSize(final CommandWrapper.WriteCommand command) {
        return command.getContent().size() == pageSize;
    }

    private boolean isPageDeleted(final long pageNumber) {
        return deletedPages.contains(pageNumber);
    }

    private boolean isPageWritten(final long pageNumber) {
        return pageMap.containsKey(pageNumber);
    }

    private boolean canWriteOneMorePage() {
        return highestWrittenPhysicalPageNumber + 1 >= pageCount;
    }

    private DeleteCommandResult processDeleteCommand(final CommandWrapper.DeleteCommand command) {
        deletedPages.add(command.getAddress());
        pageMap.remove(command.getAddress());

        return DeleteCommandResult.newBuilder()
                .setType(DeleteCommandResult.Type.ACK)
                .build();
    }

    private SealCommandResult processSealCommand(final CommandWrapper.SealCommand command) {
        if (command.getEpoch() > serverEpoch) {
            serverEpoch = command.getEpoch();
            return SealCommandResult.newBuilder()
                    .setType(SealCommandResult.Type.ACK)
                    .setHighestPageNumber(highestWrittenLogicalPageNumber)
                    .build();
        } else {
            return SealCommandResult.newBuilder()
                    .setType(SealCommandResult.Type.ERR_SEALED)
                    .build();
        }
    }
}
