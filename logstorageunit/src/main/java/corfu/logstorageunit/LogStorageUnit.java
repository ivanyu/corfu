package corfu.logstorageunit;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import corfu.logstorageunit.Protocol.*;

class LogStorageUnit {
    private final int pageSize;
    private final PhysicalStorage physicalStorage;
    private final LogicalPageMapper logicalPageMapper = new LogicalPageMapper();

    private int serverEpoch = 0;

    private static ReadCommandResult READ_RESULT_ERR_SEALED =
            ReadCommandResult.newBuilder().setType(ReadCommandResult.Type.ERR_SEALED).build();
    private static ReadCommandResult READ_RESULT_ERR_DELETED =
            ReadCommandResult.newBuilder().setType(ReadCommandResult.Type.ERR_DELETED).build();
    private static ReadCommandResult READ_RESULT_ERR_UNWRITTEN =
            ReadCommandResult.newBuilder().setType(ReadCommandResult.Type.ERR_UNWRITTEN).build();

    private static WriteCommandResult WRITE_RESULT_ACK =
            WriteCommandResult.newBuilder().setType(WriteCommandResult.Type.ACK).build();
    private static WriteCommandResult WRITE_RESULT_ERR_CONTENT_SIZE =
            WriteCommandResult.newBuilder().setType(WriteCommandResult.Type.ERR_CONTENT_SIZE).build();
    private static WriteCommandResult WRITE_RESULT_ERR_SEALED =
            WriteCommandResult.newBuilder().setType(WriteCommandResult.Type.ERR_SEALED).build();
    private static WriteCommandResult WRITE_RESULT_ERR_DELETED =
            WriteCommandResult.newBuilder().setType(WriteCommandResult.Type.ERR_DELETED).build();
    private static WriteCommandResult WRITE_RESULT_ERR_WRITTEN =
            WriteCommandResult.newBuilder().setType(WriteCommandResult.Type.ERR_WRITTEN).build();
    private static WriteCommandResult WRITE_RESULT_ERR_NO_FREE_PAGE =
            WriteCommandResult.newBuilder().setType(WriteCommandResult.Type.ERR_NO_FREE_PAGE).build();

    private static DeleteCommandResult DELETE_RESULT_ACK =
            DeleteCommandResult.newBuilder().setType(DeleteCommandResult.Type.ACK).build();

    private static SealCommandResult SEAL_RESULT_ERR_SEALED =
            SealCommandResult.newBuilder().setType(SealCommandResult.Type.ERR_SEALED).build();


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
            return READ_RESULT_ERR_SEALED;
        }
        if (logicalPageMapper.isPageDeleted(command.getPageNumber())) {
            return READ_RESULT_ERR_DELETED;
        }
        if (!logicalPageMapper.isPageWritten(command.getPageNumber())) {
            return READ_RESULT_ERR_UNWRITTEN;
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
            return WRITE_RESULT_ERR_CONTENT_SIZE;
        }
        if (isPastEpoch(command.getEpoch())) {
            return WRITE_RESULT_ERR_SEALED;
        }
        if (logicalPageMapper.isPageDeleted(command.getPageNumber())) {
            return WRITE_RESULT_ERR_DELETED;
        }
        if (logicalPageMapper.isPageWritten(command.getPageNumber())) {
            return WRITE_RESULT_ERR_WRITTEN;
        }

        final int physicalPageToWrite = physicalStorage.getAvailablePageNumber();
        if (physicalPageToWrite == -1) {
            return WRITE_RESULT_ERR_NO_FREE_PAGE;
        }

        logicalPageMapper.saveMapping(command.getPageNumber(), physicalPageToWrite);
        physicalStorage.writePage(physicalPageToWrite, command.getContent());

        return WRITE_RESULT_ACK;
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
        return DELETE_RESULT_ACK;
    }

    private SealCommandResult processSealCommand(final CommandWrapper.SealCommand command) {
        if (command.getEpoch() > serverEpoch) {
            serverEpoch = command.getEpoch();
            return SealCommandResult.newBuilder()
                    .setType(SealCommandResult.Type.ACK)
                    .setHighestPageNumber(logicalPageMapper.getHighestWrittenLogicalPageNumber())
                    .build();
        } else {
            return SEAL_RESULT_ERR_SEALED;
        }
    }
}
