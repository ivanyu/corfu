package corfu.logstorageunit.command;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CommandParserDeleteCommandTest {
    @Test
    public void parseCorrectDeleteCommand() throws Exception {
        final DeleteCommand expected = new DeleteCommand(1234);
        final InputStream inputStream = new ByteArrayInputStream("DELETE 1234".getBytes());
        final Command result = CommandParser.parse(inputStream);
        Assert.assertEquals(expected, result);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenFewerParameters() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("DELETE".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenMoreParameters() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("DELETE 1234 124312".getBytes());
        CommandParser.parse(inputStream);
    }

    @Test(expected = InvalidCommandException.class)
    public void notParseWhenNonIntegerAddress() throws Exception {
        final InputStream inputStream = new ByteArrayInputStream("DELETE aaa".getBytes());
        CommandParser.parse(inputStream);
    }
}
