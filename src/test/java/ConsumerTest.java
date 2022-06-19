import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLOutput;

public class ConsumerTest {
    @Test
    public void testConsumedEarliest() throws InvalidProtocolBufferException {
        //represent Employee entry as name:role
        String[] expected = {"Vinay:intern", "Kevin:SDE-DE", "John:HR", "Ram:Product Manager", "Tom:IT guy"};

        //produce
        Producer producer = new Producer();
        producer.produce(expected);

        //consume
        LogConsumer logConsumer = new LogConsumer();
        String[] actual = logConsumer.subscribeAndConsumeTest(expected.length);
        Assert.assertArrayEquals(expected, actual);
    }
}
