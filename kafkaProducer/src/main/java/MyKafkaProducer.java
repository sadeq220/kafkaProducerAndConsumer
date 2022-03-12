import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyKafkaProducer {
    private static final KafkaProducer<String, String> kafkaProducer;

    static {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"MyCostumeProducerInterceptor");

        kafkaProducer = new KafkaProducer<>(properties);
    }
    public static void main(String[] args) {

        try(kafkaProducer){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "javaKEY5", "javaVALUE5");
            RecordMetadata recordMetadata = synchronousSendToKafka(kafkaProducer, producerRecord);

            System.out.println(recordMetadata);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

//        Runtime.getRuntime().addShutdownHook(new Thread(){
//            @Override
//            public void run() {
//                kafkaProducer.close();
//            }
//        });
    }

    /**
     * kafka synchronous call
     * it will wait for an Acknowledgment by broker
     * blocking
     */
    public static RecordMetadata synchronousSendToKafka(KafkaProducer kafkaProducer,ProducerRecord producerRecord) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        /**
         * resend on retryable errors
         */
        try{
            future.get();
        }catch (LeaderNotAvailableException e){
            synchronousSendToKafka(kafkaProducer,producerRecord);
        }
        return future.get();
    }

    /**
     * kafka asynchronous call
     * and still handle error scenarios
     */
    public static class JavaCallbackMethodProvider implements Callback {
        private ProducerRecord producerRecord;
        public JavaCallbackMethodProvider(ProducerRecord producerRecord){
            this.producerRecord=producerRecord;
        }
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            /**
             * to guarantee reliability
             * resend on retryable errors
            */
            if(e!=null && e instanceof LeaderNotAvailableException) {
                kafkaProducer.send(producerRecord,this);
            }
        }
    }
}
