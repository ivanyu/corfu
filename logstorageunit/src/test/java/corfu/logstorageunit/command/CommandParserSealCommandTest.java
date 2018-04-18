package corfu.logstorageunit.command;

import org.junit.Assert;
import org.junit.Test;

public class CommandParserSealCommandTest {
    @Test
    public void parseCorrectSealCommand() throws Exception {
        final SealCommand expected = new SealCommand(42);
        final Command result = CommandParser.parse("SEAL 42");
        Assert.assertEquals(expected, result);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenFewerParameters() throws Exception {
        CommandParser.parse("SEAL");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenMoreParameters() throws Exception {
        CommandParser.parse("SEAL 42 1234");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerEpoch() throws Exception {
        CommandParser.parse("SEAL aaa");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonEpochIsTooLargeForInteger() throws Exception {
        CommandParser.parse(String.format("SEAL %d", Long.MAX_VALUE - 10));
    }
}
