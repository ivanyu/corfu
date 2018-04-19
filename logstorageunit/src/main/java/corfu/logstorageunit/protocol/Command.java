package corfu.logstorageunit.protocol;

import corfu.logstorageunit.Protocol;

public interface Command {
    Protocol.ProtobufCommand toProtobuf();
}
