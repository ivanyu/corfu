package corfu.logstorageunit;

import com.google.protobuf.ByteString;

import java.util.BitSet;

class PhysicalStorage {
    private final int pageSize;
    private final int pageCount;

    private byte[] bytes;

    private final BitSet writtenPages;

    PhysicalStorage(final int pageSize, final int pageCount) {
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        this.bytes = new byte[pageSize * pageCount];
        this.writtenPages = new BitSet(pageCount);
    }

    /**
     * @return an available physical page number to write to,
     *         or -1 if no available page found.
     */
    int getAvailablePageNumber() {
        final int nextClearBit = writtenPages.nextClearBit(0);
        if (nextClearBit >= pageCount) {
            return -1;
        }
        return nextClearBit;
    }

    ByteString readPage(final int pageNumber) {
        final int physicalAddress = getPhysicalAddress(pageNumber);
        return ByteString.copyFrom(bytes, physicalAddress, pageSize);
    }

    void writePage(final int pageNumber, final ByteString byteString) {
        final int physicalAddress = getPhysicalAddress(pageNumber);
        byteString.copyTo(bytes, physicalAddress);
        writtenPages.set(pageNumber);
    }

    void deletePage(final int pageNumber) {
        writtenPages.clear(pageNumber);
    }

    private int getPhysicalAddress(final int physicalPage) {
        assert physicalPage <= pageCount;
        return physicalPage * pageSize;
    }
}
