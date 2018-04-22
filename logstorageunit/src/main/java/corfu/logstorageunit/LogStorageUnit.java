package corfu.logstorageunit;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import corfu.logstorageunit.Protocol.*;

class LogStorageUnit {
    private final int pageSize;
    private final PhysicalStorage physicalStorage;
    private final LogicalPageMapper logicalPageMapper = new LogicalPageMapper();

    private int serverEpoch = 0;

    LogStorageUnit(final int pageSize, final int pageCount) {
        this.pageSize = pageSize;
        this.physicalStorage = new PhysicalStorage(pageSize, pageCount);
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

        if (logicalPageMapper.isPageDeleted(command.getPageNumber())) {
            return ReadCommandResult.newBuilder()
                    .setType(ReadCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (!logicalPageMapper.isPageWritten(command.getPageNumber())) {
            return ReadCommandResult.newBuilder()
                    .setType(ReadCommandResult.Type.ERR_UNWRITTEN)
                    .build();
        }

        final int physicalPageNumber = logicalPageMapper.getPhysicalPageNumber(command.getPageNumber());
        final ByteString content = physicalStorage.readPage(physicalPageNumber);

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

        if (logicalPageMapper.isPageDeleted(command.getPageNumber())) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_DELETED)
                    .build();
        }

        if (logicalPageMapper.isPageWritten(command.getPageNumber())) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_WRITTEN)
                    .build();
        }

        final int physicalPageToWrite = physicalStorage.getAvailablePageNumber();
        if (physicalPageToWrite == -1) {
            return WriteCommandResult.newBuilder()
                    .setType(WriteCommandResult.Type.ERR_NO_FREE_PAGE)
                    .build();
        }

        logicalPageMapper.saveMapping(command.getPageNumber(), physicalPageToWrite);
        physicalStorage.writePage(physicalPageToWrite, command.getContent());

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

    private DeleteCommandResult processDeleteCommand(final CommandWrapper.DeleteCommand command) {
        // TODO not allow to delete unwritten?

        final int physicalPageNumber = logicalPageMapper.removeMapping(command.getAddress());
        if (physicalPageNumber != -1) {
            physicalStorage.deletePage(physicalPageNumber);
        }

        return DeleteCommandResult.newBuilder()
                .setType(DeleteCommandResult.Type.ACK)
                .build();
    }

    private SealCommandResult processSealCommand(final CommandWrapper.SealCommand command) {
        if (command.getEpoch() > serverEpoch) {
            serverEpoch = command.getEpoch();
            return SealCommandResult.newBuilder()
                    .setType(SealCommandResult.Type.ACK)
                    .setHighestPageNumber(logicalPageMapper.getHighestWrittenLogicalPageNumber())
                    .build();
        } else {
            return SealCommandResult.newBuilder()
                    .setType(SealCommandResult.Type.ERR_SEALED)
                    .build();
        }
    }
}
