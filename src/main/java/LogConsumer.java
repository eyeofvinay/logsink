import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class LogConsumer {
    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        //constants
        String topic = "topic3";
        String group_id = "consumer-group-16june";
        String bootstrapServer = "127.0.0.1:9092";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer using above properties
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to topic
        consumer.subscribe(Collections.singletonList(topic));

        //start consuming
        while(true) {
            log.info("Polling...");
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, byte[]> record : records) {
                //parse protobuf from byte array
                Employee employee = Employee.parseFrom(record.value());
                log.info("key:" + record.key() + ", value:" + employee.toString());
            }
        }
    }
}
