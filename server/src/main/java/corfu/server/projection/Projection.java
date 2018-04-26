package corfu.server.projection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

public class Projection {
    private final LogRange logRange;
    private final ArrayList<StorageUnitRange> storageUnitPageRanges;

    public Projection(final LogRange logRange,
                      final Collection<StorageUnitRange> storageUnitPageRanges) {
        if (logRange == null) {
            throw new IllegalArgumentException("Log range can't be null");
        }
        if (storageUnitPageRanges == null || storageUnitPageRanges.isEmpty()) {
            throw new IllegalArgumentException("At least one storage unit page range must be present");
        }

        long totalLength = 0;
        long firstLength = -1;
        final HashSet<String> seenIds = new HashSet<>();
        for (final StorageUnitRange r : storageUnitPageRanges) {
            totalLength += r.getLength();
            if (firstLength == -1) {
                firstLength = r.getLength();
            }

            if (r.getLength() != firstLength) {
                throw new IllegalArgumentException("All storage unit page ranges must be of same length");
            }

            if (seenIds.contains(r.getStorageUnitId())) {
                throw new IllegalArgumentException("Storage unit IDs must be unique");
            }

            seenIds.add(r.getStorageUnitId());
        }

        if (totalLength != logRange.getLength()) {
            throw new IllegalArgumentException("Total storage unit page range length must be equal to " +
                    "log range length");
        }

        this.logRange = logRange;
        this.storageUnitPageRanges = new ArrayList<>(storageUnitPageRanges);
    }

    public StorageUnitPage project(final long logPosition) {
        if (logPosition < logRange.getBegin() || logPosition >= logRange.getEnd()) {
            throw new IllegalArgumentException("Log position not in range");
        }

        final long logPosition0Based = logPosition - logRange.getBegin();
        final int totalUnits = storageUnitPageRanges.size();

        final int unit = (int) logPosition0Based % totalUnits;
        final long unitBegin = storageUnitPageRanges.get(unit).getBegin();
        final long pageInUnit = unitBegin + (logPosition0Based / totalUnits);

        return new StorageUnitPage(storageUnitPageRanges.get(unit).getStorageUnitId(), pageInUnit);
    }
}
