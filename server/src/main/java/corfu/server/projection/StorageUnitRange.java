package corfu.server.projection;

import java.util.Objects;

public class StorageUnitRange extends Range {

    private final String storageUnitId;

    public StorageUnitRange(final long begin, final long end, final String storageUnitId) {
        super(begin, end);

        if (storageUnitId == null || storageUnitId.isEmpty()) {
            throw new IllegalArgumentException("Storage unit ID can't be empty");
        }

        this.storageUnitId = storageUnitId;
    }

    public String getStorageUnitId() {
        return storageUnitId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StorageUnitRange that = (StorageUnitRange) o;
        return begin == that.begin &&
                end == that.end &&
                Objects.equals(storageUnitId, that.storageUnitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageUnitId, begin, end);
    }
}
