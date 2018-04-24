package corfu.storageunit.unit;

import java.util.HashMap;
import java.util.HashSet;

class LogicalPageMapper {
    private HashMap<Long, Integer> pageMap = new HashMap<>();
    private HashSet<Long> deletedLogicalPages = new HashSet<>();
    private long highestWrittenLogicalPageNumber = -1;

    int getPhysicalPageNumber(final long logicalPageNumber) {
        return pageMap.get(logicalPageNumber);
    }

    void saveMapping(final long logicalPageNumber, final int physicalPageNumber) {
        pageMap.put(logicalPageNumber, physicalPageNumber);
        highestWrittenLogicalPageNumber = Math.max(
                highestWrittenLogicalPageNumber, logicalPageNumber);
    }

    /**
     * @return the physical page number that was stored before removing,
     *         or -1 if the mapping didn't exist.
     */
    int removeMapping(final long logicalPageNumber) {
        final Integer physicalPageNumber = pageMap.remove(logicalPageNumber);
        deletedLogicalPages.add(logicalPageNumber);

        if (physicalPageNumber == null) {
            return -1;
        }
        return physicalPageNumber;
    }

    boolean isPageDeleted(final long logicalPageNumber) {
        return deletedLogicalPages.contains(logicalPageNumber);
    }

    boolean isPageWritten(final long logicalPageNumber) {
        return pageMap.containsKey(logicalPageNumber);
    }

    long getHighestWrittenLogicalPageNumber() {
        return highestWrittenLogicalPageNumber;
    }
}
