package corfu.storageunit.unit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import corfu.storageunit.Protocol.*;

public class StorageUnit {
    private final int pageSize;
    private final PhysicalStorage physicalStorage;
    private final LogicalPageMapper logicalPageMapper = new LogicalPageMapper();

    private final ConcurrencyProtector lockMechanism;

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

    private final Timer readCommandTimer;
    private final Timer writeCommandTimer;
    private final Timer deleteCommandTimer;
    private final Timer sealCommandTimer;

    public StorageUnit(final int pageSize, final int pageCount,
                       final ConcurrencyProtector lockMechanism,
                       final MetricRegistry metricRegistry) {
        this.pageSize = pageSize;
        this.physicalStorage = new PhysicalStorage(pageSize, pageCount);
        this.lockMechanism = lockMechanism;

        readCommandTimer = metricRegistry.timer("command.read");
        writeCommandTimer = metricRegistry.timer("command.write");
        deleteCommandTimer = metricRegistry.timer("command.delete");
        sealCommandTimer = metricRegistry.timer("command.seal");
    }

    public MessageLite processCommand(final CommandWrapper commandWrapper) {
            switch (commandWrapper.getCommandCase()) {
                case READ:
                    try (final Timer.Context timerCtx = readCommandTimer.time()) {
                        return processReadCommand(commandWrapper.getRead());
                    }

                case WRITE:
                    try (final Timer.Context timerCtx = writeCommandTimer.time()) {
                        return processWriteCommand(commandWrapper.getWrite());
                    }

                case DELETE:
                    try (final Timer.Context timerCtx = deleteCommandTimer.time()) {
                        return processDeleteCommand(commandWrapper.getDelete());
                    }

                case SEAL:
                    try (final Timer.Context timerCtx = sealCommandTimer.time()) {
                        return processSealCommand(commandWrapper.getSeal());
                    }
            }
            assert false;
            return null;
        }

    private ReadCommandResult processReadCommand(final CommandWrapper.ReadCommand command) {
        final long logicalPageNumber = command.getPageNumber();
        try (final CommandProtection commandProtection = lockMechanism.protectReadCommand(logicalPageNumber)) {
            if (isPastEpoch(command.getEpoch())) {
                return READ_RESULT_ERR_SEALED;
            }
            if (logicalPageMapper.isPageDeleted(logicalPageNumber)) {
                return READ_RESULT_ERR_DELETED;
            }
            if (!logicalPageMapper.isPageWritten(logicalPageNumber)) {
                return READ_RESULT_ERR_UNWRITTEN;
            }

            final int physicalPageNumber = logicalPageMapper.getPhysicalPageNumber(logicalPageNumber);
            final ByteString content = physicalStorage.readPage(physicalPageNumber);

            return ReadCommandResult.newBuilder()
                    .setType(ReadCommandResult.Type.ACK)
                    .setContent(content)
                    .build();
        }
    }

    private WriteCommandResult processWriteCommand(final CommandWrapper.WriteCommand command) {
        if (!isCorrectContentSize(command)) {
            return WRITE_RESULT_ERR_CONTENT_SIZE;
        }

        final long logicalPageNumber = command.getPageNumber();
        try (final CommandProtection commandProtection = lockMechanism.protectWriteCommand(logicalPageNumber)) {
            if (isPastEpoch(command.getEpoch())) {
                return WRITE_RESULT_ERR_SEALED;
            }
            if (logicalPageMapper.isPageDeleted(logicalPageNumber)) {
                return WRITE_RESULT_ERR_DELETED;
            }
            if (logicalPageMapper.isPageWritten(logicalPageNumber)) {
                return WRITE_RESULT_ERR_WRITTEN;
            }

            final int physicalPageToWrite = physicalStorage.getAvailablePageNumber();
            if (physicalPageToWrite == -1) {
                return WRITE_RESULT_ERR_NO_FREE_PAGE;
            }

            logicalPageMapper.saveMapping(logicalPageNumber, physicalPageToWrite);
            physicalStorage.writePage(physicalPageToWrite, command.getContent());

            return WRITE_RESULT_ACK;
        }
    }

    private boolean isPastEpoch(final int commandEpoch) {
        return commandEpoch < serverEpoch;
    }

    private boolean isCorrectContentSize(final CommandWrapper.WriteCommand command) {
        return command.getContent().size() == pageSize;
    }

    private DeleteCommandResult processDeleteCommand(final CommandWrapper.DeleteCommand command) {
        // TODO not allow to delete unwritten?

        final long logicalPageNumber = command.getPageNumber();
        try (final CommandProtection commandProtection = lockMechanism.protectDeleteCommand(logicalPageNumber)) {
            final int physicalPageNumber = logicalPageMapper.removeMapping(command.getPageNumber());
            if (physicalPageNumber != -1) {
                physicalStorage.deletePage(physicalPageNumber);
            }
            return DELETE_RESULT_ACK;
        }
    }

    private SealCommandResult processSealCommand(final CommandWrapper.SealCommand command) {
        try (final CommandProtection commandProtection = lockMechanism.protectSealCommand()) {
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
}
