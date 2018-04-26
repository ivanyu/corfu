package corfu.server.projection;

import org.junit.Test;

public class LogRangeTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeBegin() {
        new LogRange(-1, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNegativeEnd() {
        new LogRange(1, -10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowBeginEqualOrGreaterThanEnd() {
        new LogRange(1, 1);
    }

    @Test
    public void shouldAllowCorrectRange() {
        new LogRange(0, 128);
    }
}
