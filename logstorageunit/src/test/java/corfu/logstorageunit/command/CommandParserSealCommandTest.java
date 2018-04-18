package corfu.logstorageunit.command;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CommandParserSealCommandTest {
    @Test
    public void parseCorrectSealCommand() throws Exception {
        final SealCommand expected = new SealCommand(42);
        final InputStream inputStream = new ByteArrayInputStream("SEAL 42".getBytes());
        final Command result = CommandParser.parse(inputStream);
        Assert.assertEquals(expected, result);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenFewerParameters() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("SEAL".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenMoreParameters() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("SEAL 42 1234".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerEpoch() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("SEAL aaa".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonEpochIsTooLargeForInteger() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream(
                String.format("SEAL %d", Long.MAX_VALUE - 10)
                        .getBytes());
        CommandParser.parse(inputStream);
    }
}
