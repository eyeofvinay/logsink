import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;



public class Producer {
    String topicName = "topic6";
    KafkaProducer<String, byte[]> producer;

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public Producer() {
        //constants
        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        //create producer
        producer = new KafkaProducer<>(properties);
    }

    public void produce(String name, String role) {
        //build protobuf from String data
        Employee employee = Employee.newBuilder().setName(name).setRole(role).build();
        byte[] value = employee.toByteArray();

        //create a producer record
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicName, "key1", value);

        //sending the data - asynchronous
        producer.send(producerRecord);

        //flush and close client
        producer.close();
    }

    public void produce(String []employees) {
        for(int i = 0; i < employees.length; i++) {
            //split encoded Emplyoee data into name and role
            String name = employees[i].split(":")[0];
            String role = employees[i].split(":")[1];

            //build protobuf from String data
            Employee employee = Employee.newBuilder().setName(name).setRole(role).build();
            byte[] value = employee.toByteArray();

            //create a producer record
            ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicName, "key1", value);

            //sending the data - asynchronous
            producer.send(producerRecord);
        }
        //flush and close client
        producer.close();
    }
}