package ir.sadeqcloud.stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest // integration test
@EmbeddedKafka(topics = {"test"},partitions = 1,brokerProperties = { "listeners=PLAINTEXT://localhost:9093", "port=9093" })
@PropertySource(value = "classpath:/test.properties")
class KafkaStreamApplicationTests {
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    @Test
    void contextLoads() {
        String[] topics={"test"};
        Assertions.assertArrayEquals(embeddedKafkaBroker.getTopics().toArray(),topics);
    }

}
