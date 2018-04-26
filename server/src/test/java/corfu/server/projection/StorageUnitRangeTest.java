package corfu.server.projection;

import org.junit.Test;

public class StorageUnitRangeTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeBegin() {
        new StorageUnitRange(-1, 10, "unit1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeEnd() {
        new StorageUnitRange(1, -10, "unit1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowBeginEqualOrGreaterThanEnd() {
        new StorageUnitRange(1, 1, "unit1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullStorageUnitId() {
        new StorageUnitRange(1, 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowEmptyStorageUnitId() {
        new StorageUnitRange(1, 1, "");
    }

    @Test
    public void shouldAllowCorrectRange() {
        new StorageUnitRange(0, 128, "unit1");
    }
}
