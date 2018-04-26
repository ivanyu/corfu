package corfu.server.projection;

import java.util.Objects;

public class StorageUnitPage {
    private final String storageUnitId;
    private final long pageNumber;

    StorageUnitPage(final String storageUnitId, final long pageNumber) {
        if (storageUnitId == null || storageUnitId.isEmpty()) {
            throw new IllegalArgumentException("Storage unit ID can't be empty");
        }

        this.storageUnitId = storageUnitId;
        this.pageNumber = pageNumber;
    }

    public String getStorageUnitId() {
        return storageUnitId;
    }

    public long getPageNumber() {
        return pageNumber;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StorageUnitPage that = (StorageUnitPage) o;
        return pageNumber == that.pageNumber &&
                Objects.equals(storageUnitId, that.storageUnitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageUnitId, pageNumber);
    }

    @Override
    public String toString() {
        return "StorageUnitPage{" +
                "storageUnitId='" + storageUnitId + '\'' +
                ", pageNumber=" + pageNumber +
                '}';
    }
}
