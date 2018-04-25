package corfu.storageunit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import corfu.storageunit.unit.ConcurrencyProtector;
import corfu.storageunit.unit.StorageUnit;
import corfu.storageunit.unit.RWLockConcurrencyProtector;

public class StorageUnitApp {
    private static Logger logger = LoggerFactory.getLogger(StorageUnitApp.class);

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
                        .build();
        options.addOption(pageCountOption);

        final Option totalSizeOption =
                Option.builder()
                        .longOpt("total-size")
                        .argName("total size (e.g., 256M)")
                        .type(String.class)
                        .hasArg(true)
                        .build();
        options.addOption(totalSizeOption);

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
            port = getPort(cmd);
            pageSize = getPageSize(cmd);
            pageCount = getPageCount(cmd, pageSize);
        } catch (ParseException e) {
            logger.error("Error parsing command line", e);
            formatter.printHelp("<app>", options);
            System.exit(1);
        }

        mainWithParams(port, pageSize, pageCount);
    }

    private static int getPort(final CommandLine cmd) throws ParseException {
        return (int) ((long) cmd.getParsedOptionValue("port"));
    }

    private static int getPageSize(final CommandLine cmd) throws ParseException {
        return (int) ((long) cmd.getParsedOptionValue("page-size"));
    }

    private static int getPageCount(final CommandLine cmd,
                                    final int pageSize) throws ParseException {
        if (cmd.getParsedOptionValue("page-count") != null
                && cmd.getParsedOptionValue("total-size") != null) {
            throw new ParseException("Only one of 'page-count' and 'total-size' can be set");
        }

        if (cmd.getParsedOptionValue("page-count") == null
                && cmd.getParsedOptionValue("total-size") == null) {
            throw new ParseException("At least one of 'page-count' and 'total-size' can be set");
        }

        long result = -1;
        long totalSize = -1;
        if (cmd.getParsedOptionValue("page-count") != null) {
            result = ((long) cmd.getParsedOptionValue("page-count"));
            totalSize = result * pageSize;
        } else {
            final String totalSizeStr = (String) cmd.getParsedOptionValue("total-size");
            final Matcher m = Pattern.compile("^(\\d*)\\s*([KMG])$").matcher(totalSizeStr);
            if (!m.matches()) {
                throw new ParseException("Error parsing " + totalSizeStr);
            }

            totalSize = Long.parseLong(m.group(1));
            final String unit = m.group(2);
            switch (unit) {
                case "K":
                    totalSize *= 1024;
                    break;
                case "M":
                    totalSize *= 1024 * 1024;
                    break;
                case "G":
                    totalSize *= 1024 * 1024 * 1024;
                    break;

                default:
                    throw new ParseException(String.format("Unsupported unit %s", unit));
            }

            if (totalSize % pageSize != 0) {
                throw new ParseException(String.format(
                        "Total size %s = %d is not a multiple of page size %s",
                        totalSizeStr, totalSize, pageSize)
                );
            }

            result = (totalSize / pageSize);
        }

        if (totalSize > Integer.MAX_VALUE - 8) {
            throw new ParseException(String.format("Total size %d is too big", totalSize));
        }

        return (int) result;
    }

    private static void mainWithParams(final int port,
                                       final int pageSize,
                                       final int pageCount) throws InterruptedException {
        logger.info("Running with page {} pages by {} bytes, on port {}", pageCount, pageSize, port);

        final MetricRegistry metricRegistry = new MetricRegistry();

        final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();

        final ConcurrencyProtector lockMechanism = createConcurrencyProtector(pageCount);
        final StorageUnit storageUnit = new StorageUnit(
                pageSize, pageCount, lockMechanism, metricRegistry);

        final Thread serverThread = new StorageUnitServer(port, storageUnit);
        serverThread.start();
        serverThread.join();
    }

    private static ConcurrencyProtector createConcurrencyProtector(final int pageCount) {
        int addressSpaceBuckets = (int) Math.ceil(pageCount / 16.0);
        logger.debug("{} address space buckets", addressSpaceBuckets);
        return new RWLockConcurrencyProtector(addressSpaceBuckets);
//        return new EmptyConcurrencyProtector();
    }
}
