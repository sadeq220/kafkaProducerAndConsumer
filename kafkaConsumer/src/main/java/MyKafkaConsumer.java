import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class MyKafkaConsumer {
    private static final KafkaConsumer<String,String> kafkaConsumer;
    private static Thread mainThread;
    public static final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> CONCURRENT_HASH_MAP=new ConcurrentHashMap<>();
    static {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"0");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,3);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        kafkaConsumer=new KafkaConsumer<>(properties);
    }
    public static void main(String[] args) {
        mainThread=Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // WakeupException on poll(duration) call
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }));
            kafkaConsumer.subscribe(List.of("test"),new HandleBalancing(kafkaConsumer));

        Duration duration = Duration.ofSeconds(1l);
        while(true){
            try {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(duration);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    //atomic method to employ Exactly once policy
                    doWhitConsumerRecord(consumerRecord, String.class);
                }
                // commit the latest offset returned by poll(duration)
                kafkaConsumer.commitSync();
            }catch (WakeupException e){
                // just for exit code 0
            }finally {
                kafkaConsumer.close();
            }
        }
    }
    //TODO write lock-free atomic method
    private static synchronized <E> void doWhitConsumerRecord(ConsumerRecord<E,E> consumerRecord,Class<E> eClass){
        System.out.println(consumerRecord.value());
        MyKafkaConsumer.CONCURRENT_HASH_MAP.put(new TopicPartition(consumerRecord.topic(),consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset()+1,null));
    }
}
