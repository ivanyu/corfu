package corfu.logstorageunit;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import corfu.logstorageunit.unit.ConcurrencyProtector;
import corfu.logstorageunit.unit.LogStorageUnit;
import corfu.logstorageunit.unit.RWLockConcurrencyProtector;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(final String[] args) throws InterruptedException, InvalidProtocolBufferException {
        final Options options = new Options();

        final Option pageSizeOption =
                Option.builder()
                        .longOpt("page-size")
                        .argName("page size")
                        .type(Number.class)
                        .hasArg(true)
                        .required()
                        .build();
        options.addOption(pageSizeOption);

        final Option pageCountOption =
                Option.builder()
                        .longOpt("page-count")
                        .argName("page count")
                        .type(Number.class)
                        .hasArg(true)
                        .required()
                        .build();
        options.addOption(pageCountOption);

        final Option portOption =
                Option.builder("p")
                        .longOpt("port")
                        .argName("port")
                        .type(Number.class)
                        .hasArg(true)
                        .required()
                        .build();
        options.addOption(portOption);

        int port = -1;
        int pageSize = -1;
        int pageCount = -1;
        final CommandLineParser parser = new DefaultParser();
        final HelpFormatter formatter = new HelpFormatter();
        final CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            port = (int) ((long) cmd.getParsedOptionValue("port"));
            pageSize = (int) ((long) cmd.getParsedOptionValue("page-size"));
            pageCount = (int) ((long) cmd.getParsedOptionValue("page-count"));
        } catch (ParseException e) {
            logger.error("Error parsing command line", e);
            System.out.println(e.getMessage());
            formatter.printHelp("<app>", options);
            System.exit(1);
        }

        mainWithParams(port, pageSize, pageCount);
    }

    private static void mainWithParams(final int port,
                                       final int pageSize,
                                       final int pageCount) throws InterruptedException {
        logger.info("Running with page {} pages by {} bytes, on port {}", pageCount, pageSize, port);

        final ConcurrencyProtector lockMechanism = createConcurrencyProtector(pageCount);
        final LogStorageUnit logStorageUnit = new LogStorageUnit(
                pageSize, pageCount, lockMechanism);

        final Thread serverThread = new LogStorageUnitServer(port, logStorageUnit);
        serverThread.start();
        serverThread.join();
    }

    private static ConcurrencyProtector createConcurrencyProtector(final int pageCount) {
        int addressSpaceBuckets = (int) Math.ceil(pageCount / 16.0);
        logger.debug("{} address space buckets", addressSpaceBuckets);
        return new RWLockConcurrencyProtector(addressSpaceBuckets);
    }
}
