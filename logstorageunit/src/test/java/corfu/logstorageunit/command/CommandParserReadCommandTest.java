package corfu.logstorageunit.command;

import org.junit.Assert;
import org.junit.Test;

public class CommandParserReadCommandTest {
    @Test
    public void parseCorrectReadCommand() throws Exception {
        final ReadCommand expected = new ReadCommand(42, 1234);
        final Command result = CommandParser.parse("READ 42 1234");
        Assert.assertEquals(expected, result);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenFewerParameters() throws Exception {
        CommandParser.parse("READ 42");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenMoreParameters() throws Exception {
        CommandParser.parse("READ 42 1234 124312");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerEpoch() throws Exception {
        CommandParser.parse("READ aaa 1234");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerAddress() throws Exception {
        CommandParser.parse("READ 42 aaa");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonEpochIsTooLargeForInteger() throws Exception {
        CommandParser.parse(String.format("READ %d 1234", Long.MAX_VALUE - 10));
    }
}
