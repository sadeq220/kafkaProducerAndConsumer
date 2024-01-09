import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyKafkaConsumer {
    private static final KafkaConsumer<String,String> kafkaConsumer;// consumer object is not thread safe
    private static Thread mainThread;
    public static final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> OFFSET_TRACKER =new ConcurrentHashMap<>();
    private static Pattern regexPattern = Pattern.compile("^.*?(\\.*)$");
    static {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"0");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");// read AUTO_OFFSET_RESET_DOC
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,3);// to make sure we don't ran out of memory
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,20_000);
        /**
         * Exactly-once semantic on rebalance
         * 1 ) Auto_Commit_Offset = true
         * every five seconds the consumer will commit the largest offset your client received from poll().
         * The five-second interval is the default and is controlled by setting auto.commit.interval.ms.
         * Just like every thing else in the consumer, the automatic commits are driven by the poll loop.
         * When ever you poll, the consumer checks if it is time to commit, and if it is, it will commit the offsets it returned in the last poll.
         *
         * 2 ) Auto_Commit_Offset = false
         * offsets will only be committed when the application explicitly chooses to do so.
         *      2_1) commitSync()
         *          commit the latest offset returned by poll() and return once the offset is committed.
         *      2_2) commitAsync(callback)
         *          commit the last offset , without retry on transient errors
         *
         * Note: always use commitSync() on shutdown logic or rebalance listeners
         *      2_3) commitSync(map<TopicPartition,OffsetAndMetadata>)
         *          to commit a specified offset.
         *          to commit offsets in the middle of the batch.
         *
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.CooperativeStickyAssignor");//Avoid “Eager Rebalances”(because they will suspend a whole topic consumption) by using “Cooperative” Rebalances(incremental rebalances)
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,2_000);//fetch (api_key=1) max wait time ,default is 500ms , you must comply with expression ' pollDuration > fetch_max_wait ' as it cause multiple poll loop execution without even Fetch response came back

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

        Duration duration = Duration.ofSeconds(3l);
        try {
        while(true){

                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(duration);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    //atomic method to employ Exactly once policy
                    doWhitConsumerRecord(consumerRecord, String.class);
                    kafkaConsumer.commitAsync(OFFSET_TRACKER,null);
                }
                // commit the latest offset returned by poll(duration)
               // kafkaConsumer.commitSync();// it will raise RebalanceInProgressException
        }}catch (WakeupException e){
            // just for exit code 0
        }finally {
            kafkaConsumer.commitSync(OFFSET_TRACKER);
            kafkaConsumer.close();
        }
    }
    /** lock-free atomic method - CAS  (Compare And Swap)
        is widely used in the lock-free algorithms that can leverage the CAS processor instruction to provide great speedup compared to the standard pessimistic synchronization mechanism in Java
     */
    private static <E> void doWhitConsumerRecord(ConsumerRecord<E,E> consumerRecord,Class<E> eClass){
        MyKafkaConsumer.OFFSET_TRACKER.compute(new TopicPartition(consumerRecord.topic(),consumerRecord.partition()),(k, v)->{
            System.out.println(k+"    "+consumerRecord.value());
            if (v != null && v.offset()>consumerRecord.offset()){
                throw new RuntimeException("Stale offset update!");
            }
            String recordValue = consumerRecord.value().toString();
            doWithRecordValue(recordValue);
            System.out.println("processing the record is done!");
            return new OffsetAndMetadata(consumerRecord.offset()+1,null);});
    }

    /**
     * Sleeps for the number of seconds indicated by the trailing dots in the input string.
     */
    private static void doWithRecordValue(String recordValue) {
        Matcher matcher = regexPattern.matcher(recordValue);
        if (matcher.find()) {
            String group = matcher.group(1);
            int length = group.length();
            try {
                if (length != 0)
                    Thread.sleep(Duration.ofSeconds(length).toMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
