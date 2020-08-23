import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

import com.learning.kafka.client.GenericKafkaProducer;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;

public class GenericKafkaProducerTest {

    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    @Test
    public void testStringSerializer() {
        Properties kafkaProperties = getKafkaProducerProperties("org.apache.kafka.common.serialization.StringSerializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        GenericKafkaProducer<String, String> genericKafkaProducer = new GenericKafkaProducer<>(kafkaProperties);
        genericKafkaProducer.send("samples", UUID.randomUUID().toString(), "Hello Kafka").join();
    }

    @Test
    public void testByteArraySerializer() {
        Properties kafkaProperties = getKafkaProducerProperties("org.apache.kafka.common.serialization.StringSerializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        GenericKafkaProducer<String, byte[]> genericKafkaProducer = new GenericKafkaProducer<>(kafkaProperties);
        genericKafkaProducer.send("samples", UUID.randomUUID().toString(), "Hello Kafka".getBytes(StandardCharsets.UTF_8)).join();
    }

    /**
     * Returns a Properties object for Kafka producer to send messages to Kafka
     * @param keySerializer - Class used to serialize keys in kafka
     * @param valueSerializer - Class used to serialize values in kafka
     * @return
     */
    private Properties getKafkaProducerProperties(String keySerializer, String valueSerializer) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return kafkaProperties;
    }
}
