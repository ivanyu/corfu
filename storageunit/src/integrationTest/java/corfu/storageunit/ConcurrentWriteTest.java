package corfu.storageunit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import com.codahale.metrics.MetricRegistry;

import corfu.storageunit.protocol.CommandFactory;
import corfu.storageunit.unit.ConcurrencyProtector;
import corfu.storageunit.unit.RWLockConcurrencyProtector;
import corfu.storageunit.unit.StorageUnit;

public class ConcurrentWriteTest {
    private static int CONCURRENT_WRITERS = 100;
    private static int PAGE_SIZE = 1024;
    private static int PAGE_COUNT = 256;
    private static int ITERATIONS = PAGE_COUNT;

    private StorageUnitServer server;
    private Socket[] clientSockets = new Socket[CONCURRENT_WRITERS];
    private Socket controlSocket;

    @Before
    public void before() throws Exception {
        // 1 sync bucket to ensure contention
        final ConcurrencyProtector lockMechanism = new RWLockConcurrencyProtector(1);
        final MetricRegistry metricRegistry = new MetricRegistry();
        final StorageUnit storageUnit =
                new StorageUnit(PAGE_SIZE, PAGE_COUNT, lockMechanism, metricRegistry);
        server = new StorageUnitServer(0, storageUnit);
        server.start();

        while (!server.isStarted()) {
            Thread.sleep(10);
        }

        for (int i = 0; i < CONCURRENT_WRITERS; i++) {
            clientSockets[i] = new Socket();
            clientSockets[i].connect(new InetSocketAddress("localhost", server.getPort()));
        }
        controlSocket = new Socket();
        controlSocket.connect(new InetSocketAddress("localhost", server.getPort()));
    }

    @After
    public void after() throws Exception {
        for (int i = 0; i < CONCURRENT_WRITERS; i++) {
            clientSockets[i].close();
        }
        controlSocket.close();
        server.interrupt();
    }

    @Test(timeout = 30000)
    public void test() throws Exception {
        // Pre-write and post-write barrier.
        final CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_WRITERS + 1);
        final AtomicInteger successWriteCounter = new AtomicInteger(0);
        final AtomicInteger errorWriteCounter = new AtomicInteger(0);

        // Start threads.
        final WriterThread[] threads = new WriterThread[CONCURRENT_WRITERS];
        for (int i = 0; i < CONCURRENT_WRITERS; i++) {
            threads[i] = new WriterThread(
                    clientSockets[i], barrier, ITERATIONS,
                    successWriteCounter, errorWriteCounter
            );
            threads[i].start();
        }

        try(final OutputStream os = controlSocket.getOutputStream();
            final InputStream is = controlSocket.getInputStream()) {
            for (int pageNumber = 0; pageNumber < ITERATIONS; pageNumber++) {
                successWriteCounter.set(0);
                errorWriteCounter.set(0);

                barrier.await(); // Start writing
                barrier.await(); // Finish writing

                // One must always succeed in writing.
                Assert.assertEquals(1, successWriteCounter.get());
                // The rest must always fail.
                Assert.assertEquals(CONCURRENT_WRITERS - 1, errorWriteCounter.get());

                checkValidityOfWrittenPage(os, is, pageNumber);
            }
        }

        for (int i = 0; i < CONCURRENT_WRITERS; i++) {
            threads[i].join();
        }
    }

    private static class WriterThread extends Thread {
        private final Socket socket;
        private final CyclicBarrier barrier;
        private final int times;
        private final AtomicInteger successWriteCounter;
        private final AtomicInteger errorWriteCounter;

        private WriterThread(final Socket socket,
                             final CyclicBarrier barrier,
                             final int times,
                             final AtomicInteger successWriteCounter,
                             final AtomicInteger errorWriteCounter) {
            this.socket = socket;
            this.barrier = barrier;
            this.times = times;
            this.successWriteCounter = successWriteCounter;
            this.errorWriteCounter = errorWriteCounter;
        }

        @Override
        public void run() {
            try(final OutputStream os = socket.getOutputStream();
                final InputStream is = socket.getInputStream()) {
                for (int pageNumber = 0; pageNumber < times; pageNumber++) {
                    final byte[] pageToWrite = generatePageWithCrc32();

                    barrier.await();

                    final Protocol.WriteCommandResult writeCommandResult =
                            writePage(pageNumber, pageToWrite, os, is);

                    switch (writeCommandResult.getType()) {
                        case ACK:
                            successWriteCounter.incrementAndGet();
                            break;

                        case ERR_WRITTEN:
                            errorWriteCounter.incrementAndGet();
                            break;

                        default:
                            throw new Exception("Shouldn't ever have " + writeCommandResult.getType());
                    }

                    barrier.await();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static byte[] generatePageWithCrc32() {
        // Page size with Long length bytes left for CRC32;
        final byte[] randomBytes = new byte[PAGE_SIZE - Long.BYTES];
        new Random().nextBytes(randomBytes);

        final CRC32 crc32 = new CRC32();
        crc32.update(randomBytes);

        final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        buf.put(randomBytes);
        buf.putLong(crc32.getValue());

        return buf.array();
    }

    private static void checkValidityOfWrittenPage(final OutputStream os, final InputStream is, final int pageNumber) throws IOException {
        CommandFactory.createReadCommand(0, pageNumber)
                .writeDelimitedTo(os);
        final Protocol.ReadCommandResult readCommandResult = Protocol.ReadCommandResult.parseDelimitedFrom(is);
        Assert.assertEquals(Protocol.ReadCommandResult.Type.ACK, readCommandResult.getType());

        final ByteBuffer buf = ByteBuffer.wrap(readCommandResult.getContent().toByteArray());

        final byte[] bytesToCheck = new byte[PAGE_SIZE - Long.BYTES];
        buf.get(bytesToCheck, 0, PAGE_SIZE - Long.BYTES);

        final CRC32 crc32 = new CRC32();
        crc32.update(bytesToCheck);

        final long expectedCrc32Value = buf.getLong();
        Assert.assertEquals(crc32.getValue(), expectedCrc32Value);
    }

    private static Protocol.WriteCommandResult writePage(
            final int pageNumber, final byte[] pageToWrite,
            final OutputStream os, final InputStream is) throws IOException {
        CommandFactory.createWriteCommand(0, pageNumber, pageToWrite)
                .writeDelimitedTo(os);
        return Protocol.WriteCommandResult.parseDelimitedFrom(is);
    }
}
