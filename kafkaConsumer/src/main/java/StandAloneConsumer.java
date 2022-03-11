import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class StandAloneConsumer {
    public static final KafkaConsumer<String,String> kafkaConsumer;
    static {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,3);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        kafkaConsumer=new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(Thread.currentThread(),kafkaConsumer));
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("test");
        PartitionInfo partitionInfo=partitionInfos.get(0);
        TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
        /**
         * A consumer can either subscribe to topics (and be part of a consumer group),
         * or assign itself partitions,
         * but not both at the same time.
         */
        kafkaConsumer.assign(Set.of(topicPartition));

        while(true){
            try {
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(2L));

            }catch (WakeupException e){
        }finally {
                kafkaConsumer.close();
            }
            }

    }
}
