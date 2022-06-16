import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;



public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        //constants
        String topicName = "topic3";
        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        //create producer
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        //build protobuf object and convert to bytearray
        //Employee employee = Employee.newBuilder().setName("Kevin B").setRole("DE--SDE").build();
        Employee employee = Employee.newBuilder().setName("Vinay V").setRole("DE--Intern").build();
        byte[] value = employee.toByteArray();

        //create a producer record
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicName, value);

        //sending the data - asynchronous
        producer.send(producerRecord);

        //flush and close client
        producer.close();
    }
}