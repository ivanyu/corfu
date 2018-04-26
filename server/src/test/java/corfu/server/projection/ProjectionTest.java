package corfu.server.projection;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class ProjectionTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullLogPositionRange() {
        new Projection(
                null,
                Arrays.asList(new StorageUnitRange(0, 128, "unit1"))
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullStorageUnitPageRanges() {
        new Projection(
                new LogRange(0, 128),
                null
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowEmptyStorageUnitPageRanges() {
        new Projection(
                new LogRange(0, 128),
                Collections.emptyList()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowStorageUnitPageRangesOfDifferentLengths() {
        new Projection(
                new LogRange(0, 128),
                Arrays.asList(
                        new StorageUnitRange(0, 100, "unit1"),
                        new StorageUnitRange(0, 28, "unit2")
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowStorageUnitPageRangesWithNonUniqueIds() {
        new Projection(
                new LogRange(0, 128),
                Arrays.asList(
                        new StorageUnitRange(0, 64, "unit1"),
                        new StorageUnitRange(0, 64, "unit1")
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowStorageUnitPageRangesWithTotalLengthDifferentFromLogRange() {
        new Projection(
                new LogRange(0, 128),
                Arrays.asList(
                        new StorageUnitRange(0, 64, "unit1"),
                        new StorageUnitRange(0, 65, "unit2")
                )
        );
    }

    @Test
    public void shouldAllowCorrectProjection() {
        new Projection(
                new LogRange(0, 128),
                Arrays.asList(
                        new StorageUnitRange(0, 64, "unit1"),
                        new StorageUnitRange(0, 64, "unit2")
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotDoProjectionForPositionBeforeLogRange() {
        final Projection projection =
                new Projection(
                        new LogRange(128, 256),
                        Arrays.asList(
                                new StorageUnitRange(0, 64, "unit1"),
                                new StorageUnitRange(0, 64, "unit2")
                        )
                );

        projection.project(127);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotDoProjectionForPositionAfterLogRange() {
        final Projection projection =
                new Projection(
                        new LogRange(128, 256),
                        Arrays.asList(
                                new StorageUnitRange(0, 64, "unit1"),
                                new StorageUnitRange(0, 64, "unit2")
                        )
                );

        projection.project(257);
    }

    @Test
    public void shouldDoCorrectProjectionsForPositionInLogRange() {
        final Projection projection =
                new Projection(
                        new LogRange(1, 301),
                        Arrays.asList(
                                new StorageUnitRange(0, 100, "unit1"),
                                new StorageUnitRange(1000, 1100, "unit2"),
                                new StorageUnitRange(13, 113, "unit3")
                        )
                );

        final StorageUnitPage expected1 = new StorageUnitPage("unit1", 0);
        final StorageUnitPage storageUnitPage1 = projection.project(1);
        Assert.assertEquals(expected1, storageUnitPage1);

        final StorageUnitPage expected2 = new StorageUnitPage("unit2", 1000);
        final StorageUnitPage storageUnitPage2 = projection.project(2);
        Assert.assertEquals(expected2, storageUnitPage2);

        final StorageUnitPage expected3 = new StorageUnitPage("unit3", 13);
        final StorageUnitPage storageUnitPage3 = projection.project(3);
        Assert.assertEquals(expected3, storageUnitPage3);

        final StorageUnitPage expected4 = new StorageUnitPage("unit1", 1);
        final StorageUnitPage storageUnitPage4 = projection.project(4);
        Assert.assertEquals(expected4, storageUnitPage4);

        final StorageUnitPage expected5 = new StorageUnitPage("unit2", 1001);
        final StorageUnitPage storageUnitPage5 = projection.project(5);
        Assert.assertEquals(expected5, storageUnitPage5);

        final StorageUnitPage expected6 = new StorageUnitPage("unit3", 14);
        final StorageUnitPage storageUnitPage6 = projection.project(6);
        Assert.assertEquals(expected6, storageUnitPage6);

        final StorageUnitPage expected7 = new StorageUnitPage("unit1", 2);
        final StorageUnitPage storageUnitPage7 = projection.project(7);
        Assert.assertEquals(expected7, storageUnitPage7);

        final StorageUnitPage expected8 = new StorageUnitPage("unit2", 1002);
        final StorageUnitPage storageUnitPage8 = projection.project(8);
        Assert.assertEquals(expected8, storageUnitPage8);

        final StorageUnitPage expected9 = new StorageUnitPage("unit3", 15);
        final StorageUnitPage storageUnitPage9 = projection.project(9);
        Assert.assertEquals(expected9, storageUnitPage9);

        final StorageUnitPage expected10 = new StorageUnitPage("unit1", 3);
        final StorageUnitPage storageUnitPage10 = projection.project(10);
        Assert.assertEquals(expected10, storageUnitPage10);

        final StorageUnitPage expected11 = new StorageUnitPage("unit2", 1003);
        final StorageUnitPage storageUnitPage11 = projection.project(11);
        Assert.assertEquals(expected11, storageUnitPage11);

        final StorageUnitPage expected12 = new StorageUnitPage("unit3", 16);
        final StorageUnitPage storageUnitPage12 = projection.project(12);
        Assert.assertEquals(expected12, storageUnitPage12);
    }
}
