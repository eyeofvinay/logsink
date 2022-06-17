import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLOutput;

public class ConsumerTest {
    @Test
    public void testConsumedEarliest() {
        String[] expected = {"Gojek", "Vinay", "Kafka-Logsink", "Data Engineering"};
        LogConsumerString logConsumerString = new LogConsumerString();
        String[] actual = logConsumerString.consumeTest(expected.length);
        for(int i = 0; i < expected.length; i++) {
            System.out.println("should be:" + expected[i] + ", found:" + actual[i]);
            if(!expected[i].equals(actual[i])) {
                Assert.assertTrue(false);
                return;
            }
        }
        Assert.assertTrue(true);
    }
}
