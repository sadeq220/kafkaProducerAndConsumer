import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import sun.misc.Unsafe;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class MyKafkaConsumer {
    private static final KafkaConsumer<String,String> kafkaConsumer;// consumer object is not thread safe
    private static Thread mainThread;
    public static final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> CONCURRENT_HASH_MAP=new ConcurrentHashMap<>();
    static {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"0");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");// read AUTO_OFFSET_RESET_DOC
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,3);// to make sure we don't ran out of memory
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.CooperativeStickyAssignor");//Avoid “stop-the-world” consumer group rebalances by using cooperative rebalancing

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
        try {
        while(true){

                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(duration);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    //atomic method to employ Exactly once policy
                    doWhitConsumerRecord(consumerRecord, String.class);
                }
                // commit the latest offset returned by poll(duration)
                kafkaConsumer.commitSync();
        }}catch (WakeupException e){
            // just for exit code 0
        }finally {
            kafkaConsumer.close();
        }
    }
    /** lock-free atomic method - CAS  (Compare And Swap)
        is widely used in the lock-free algorithms that can leverage the CAS processor instruction to provide great speedup compared to the standard pessimistic synchronization mechanism in Java
     */
    private static <E> void doWhitConsumerRecord(ConsumerRecord<E,E> consumerRecord,Class<E> eClass){
        MyKafkaConsumer.CONCURRENT_HASH_MAP.compute(new TopicPartition(consumerRecord.topic(),consumerRecord.partition()),(k,v)->{
            System.out.println(consumerRecord.value());
        return new OffsetAndMetadata(consumerRecord.offset()+1,null);});
    }
}
