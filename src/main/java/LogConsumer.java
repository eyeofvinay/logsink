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
    String topicName;
    String group_id;
    String bootstrapServer;
    String autoOffsetRest;
    KafkaConsumer<String, byte[]> consumer;
    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class.getSimpleName());

    public LogConsumer(String group_id, String topicName, String bootstrapServer, String autoOffsetRest) {
        this.topicName = topicName;
        this.group_id = group_id;
        this.bootstrapServer = bootstrapServer;
        this.autoOffsetRest = autoOffsetRest;


        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetRest);

        //create consumer using above properties
        consumer = new KafkaConsumer<>(properties);
    }

    public String subscribeAndConsume() throws InvalidProtocolBufferException {
        //subscribe consumer to topic
        consumer.subscribe(Collections.singletonList(topicName));

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

    public Employee[] subscribeAndConsumeTest(int numberOfMessages) throws InvalidProtocolBufferException {
        Employee[] messages = new Employee[numberOfMessages];

        //subscribe consumer to topic
        consumer.subscribe(Collections.singletonList(topicName));

        //start consuming
        int i = 0;
        while(true) {
            log.info("Polling...");
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()) {
                for(ConsumerRecord<String, byte[]> record : records) {
                    //parse protobuf from byte array
                    Employee employee = Employee.parseFrom(record.value());
                    log.info("KEY-" + record.key() + ", VALUE-" + employee.toString());
                    messages[i++] = employee;
                    if(i == numberOfMessages) {
                        consumer.close();
                        return messages;
                    }
                }
            }

        }
    }
}
