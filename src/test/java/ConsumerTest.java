import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLOutput;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class ConsumerTest {
    String topicName;
    String bootStrapServer;

    @Before
    public void topicSetup() {
        topicName = "topic-" + new Random().nextInt(1000);
        bootStrapServer = "127.0.0.1:9092";
        int NUM_PARTITIONS = 3;
        short REPLICATION_FACTOR = 1;

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        AdminClient adminClient = AdminClient.create(properties);

        //create topic
        CreateTopicsResult newTopic = adminClient.createTopics(Collections.singleton(new NewTopic(topicName, NUM_PARTITIONS, REPLICATION_FACTOR)));
        adminClient.close(Duration.ofSeconds(30));
    }

    @Test
    public void testConsumedEarliest() throws InvalidProtocolBufferException {
        //represent Employee entry as name:role
        String[] expectedData = {"Vinay:intern", "Kevin:SDE-DE", "John:HR", "Ram:Product Manager", "Tom:IT guy"};
        int N = expectedData.length;

        //produce
        Producer producer = new Producer(topicName, bootStrapServer);
        Employee[] expected = mapStringToProtobuf(expectedData); //strings to protobufs
        producer.produce(expected);

        //consume
        String randomConsumerGroup = "consumer-group-" + new Random().nextInt(1000);
        LogConsumer logConsumer = new LogConsumer(randomConsumerGroup, topicName, bootStrapServer, "earliest");
        Employee[] actual = logConsumer.subscribeAndConsumeTest(expected.length);
        Assert.assertArrayEquals(expected, actual);
    }
    @After
    public void destroy() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        AdminClient adminClient = AdminClient.create(properties);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicName));
        deleteTopicsResult.all().get();
    }

    private Employee[] mapStringToProtobuf(String[] strings) {
        Employee[] protobufs = new Employee[strings.length];
        for(int i = 0; i < strings.length; i++) {
            //split encoded Employee data into name and role
            String name = strings[i].split(":")[0];
            String role = strings[i].split(":")[1];
            protobufs[i] = Employee.newBuilder().setName(name).setRole(role).build();;
        }
        return protobufs;
    }
}
