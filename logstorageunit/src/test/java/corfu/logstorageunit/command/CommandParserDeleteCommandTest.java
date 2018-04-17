package corfu.logstorageunit.command;

import org.junit.Assert;
import org.junit.Test;

public class CommandParserDeleteCommandTest {
    @Test
    public void parseCorrectDeleteCommand() throws Exception {
        final DeleteCommand expected = new DeleteCommand(1234);
        final Command result = CommandParser.parse("DELETE 1234");
        Assert.assertEquals(expected, result);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenFewerParameters() throws Exception {
        CommandParser.parse("DELETE");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenMoreParameters() throws Exception {
        CommandParser.parse("DELETE 1234 124312");
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerAddress() throws Exception {
        CommandParser.parse("DELETE aaa");
    }
}
