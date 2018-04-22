package corfu.logstorageunit.protocol;

import com.google.protobuf.ByteString;
import corfu.logstorageunit.Protocol.CommandWrapper;

public final class CommandFactory {
    public static CommandWrapper createReadCommand(final int epoch, final long pageNumber) {
        final CommandWrapper.ReadCommand readCommand =
                CommandWrapper.ReadCommand.newBuilder()
                        .setEpoch(epoch)
                        .setPageNumber(pageNumber)
                        .build();
        return CommandWrapper.newBuilder()
                .setRead(readCommand)
                .build();
    }

    public static CommandWrapper createWriteCommand(final int epoch,
                                                    final long pageNumber,
                                                    final byte[] content) {
        final CommandWrapper.WriteCommand writeCommand =
                CommandWrapper.WriteCommand.newBuilder()
                        .setEpoch(epoch)
                        .setPageNumber(pageNumber)
                        .setContent(ByteString.copyFrom(content))
                        .build();
        return CommandWrapper.newBuilder()
                .setWrite(writeCommand)
                .build();
    }

    public static CommandWrapper createDeleteCommand(final long pageNumber) {
        final CommandWrapper.DeleteCommand deleteCommand =
                CommandWrapper.DeleteCommand.newBuilder()
                        .setPageNumber(pageNumber)
                        .build();
        return CommandWrapper.newBuilder()
                .setDelete(deleteCommand)
                .build();
    }

    public static CommandWrapper createSealCommand(final int epoch) {
        final CommandWrapper.SealCommand sealCommand =
                CommandWrapper.SealCommand.newBuilder()
                        .setEpoch(epoch)
                        .build();
        return CommandWrapper.newBuilder()
                .setSeal(sealCommand)
                .build();
    }
}
