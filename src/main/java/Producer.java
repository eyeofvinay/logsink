import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;



public class Producer {
    String topicName;
    String bootStrapServer;
    KafkaProducer<String, byte[]> producer;

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public Producer(String topicName, String bootStrapServer) {
        this.topicName = topicName;
        this.bootStrapServer = bootStrapServer;

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        //create producer
        producer = new KafkaProducer<>(properties);
    }

    public void produce(String name, String role) {
        //build protobuf from String data
        Employee employee = Employee.newBuilder().setName(name).setRole(role).build();

        //create a producer record
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicName, "key1", employee.toByteArray());

        //sending the data - asynchronous
        producer.send(producerRecord);

        //flush and close client
        producer.close();
    }

    public void produce(Employee []employees) {
        for(int i = 0; i < employees.length; i++) {
            //protobuf to bytearray
            byte[] value = employees[i].toByteArray();

            //create a producer record
            ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicName, "key1", value);

            //sending the data - asynchronous
            producer.send(producerRecord);
        }
        //flush and close client
        producer.close();
    }
}