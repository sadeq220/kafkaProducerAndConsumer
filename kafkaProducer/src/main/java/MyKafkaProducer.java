import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyKafkaProducer {
    private static final KafkaProducer<String, String> kafkaProducer;

    static {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        /**
         * Message Delivery Time
         * The upper bound time to get ack from the broker .
         * this time comprises linger.ms , retry.backoff.ms , request.timeout.ms .
         * effectively ( retry time + in-flight time )
         * Use this time to manage producer retry manner .
         * note: in case of leader election it will take up to 30 sec
         */
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,120_000);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"MyCostumeProducerInterceptor");
        /**
         * Idempotent producer ( part of Exactly-Once-Semantic )
         * send sequence number and producer Id along message to identify duplicate messages by the broker.
         * this guarantees message ordering and also guarantees that retries will not introduce duplicates.
         * If the broker receives records with the same sequence number within a 5 message window,
         * it will reject the second copy and the producer will receive the harmless DuplicateSequenceException.
         *
         * Enabling idempotence requires max.in.flight.requests.per.connection
         * to be less than or equal to 5, retries to be greater than 0
         * (either directly or via delivery.timeout.ms) and acks=all .
         * incompatible values are set, a ConfigException will be thrown.
         */
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,15_000);//kafka.timeout , default is 30s , wait time for a reply from the broker

        kafkaProducer = new KafkaProducer<>(properties);
    }
    public static void main(String[] args) {
        try (kafkaProducer) {
        while(true) {
            Scanner scanner = new Scanner(System.in);
            System.out.print("message key : ");
            String key = scanner.next();
            System.out.print("message value : ");
            String value = scanner.next();

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("Test_Topic_Creation", key, value);
                RecordMetadata recordMetadata = synchronousSendToKafka(kafkaProducer, producerRecord);

                System.out.println(recordMetadata);


//        Runtime.getRuntime().addShutdownHook(new Thread(){
//            @Override
//            public void run() {
//                kafkaProducer.close();
//            }
//        });
        }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * kafka synchronous call
     * it will wait for an Acknowledgment by broker
     * blocking
     */
    public static RecordMetadata synchronousSendToKafka(KafkaProducer kafkaProducer,ProducerRecord producerRecord) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        /**
         * producer built-in retries resend on retryable errors
         */
          return  future.get();
    }

    /**
     * kafka asynchronous call
     * and still handle error scenarios
     * The callbacks execute in the producer’s main thread to achieve ordered execution of callbacks
     */
    public static class JavaCallbackMethodProvider implements Callback {
        private ProducerRecord producerRecord;
        public JavaCallbackMethodProvider(ProducerRecord producerRecord){
            this.producerRecord=producerRecord;
        }
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            /**
             * to guarantee reliability , producer will
             * resend on retryable errors(transient errors e.g. LeaderNotAvailableException)
            */
            /**
             * It is always a good idea to use the built-in retry mechanism of the producer
             */
            if (e != null) // log non-retryable errors
                e.printStackTrace();
        }
    }
}
