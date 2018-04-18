package corfu.logstorageunit.command;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CommandParserReadCommandTest {
    @Test
    public void parseCorrectReadCommand() throws Exception {
        final ReadCommand expected = new ReadCommand(42, 1234);
        final InputStream inputStream = new ByteArrayInputStream("READ 42 1234".getBytes());
        final Command result = CommandParser.parse(inputStream);
        Assert.assertEquals(expected, result);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenFewerParameters() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("READ 42".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenMoreParameters() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("READ 42 1234 124312".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerEpoch() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("READ aaa 1234".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerAddress() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("READ 42 aaa".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonEpochIsTooLargeForInteger() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream(
                String.format("READ %d 1234", Long.MAX_VALUE - 10)
                        .getBytes());
        CommandParser.parse(inputStream);
    }
}
