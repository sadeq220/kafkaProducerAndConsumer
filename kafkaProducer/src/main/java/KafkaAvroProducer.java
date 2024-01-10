import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaAvroProducer {
    private static final KafkaProducer<String, GenericData.Record> kafkaProducer;

    static {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,120_000);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"MyCostumeProducerInterceptor");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,15_000);//kafka.timeout , default is 30s , wait time for a reply from the broker
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProducer = new KafkaProducer<>(properties);
    }
    public static void main(String[] args) {
        try {
            Schema avroSchema = dynamicSchema();
            GenericData.Record avroRecord = new GenericRecordBuilder(avroSchema).set("name", "sadeq").set("favourite_number", 7).build();
            ProducerRecord<String, GenericData.Record> kafkaRecord = new ProducerRecord<>("avro", "transaction", avroRecord);
            Future<RecordMetadata> brokerRespond = kafkaProducer.send(kafkaRecord);
            RecordMetadata recordMetadata = brokerRespond.get();
            System.out.println(recordMetadata);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
    private static Schema dynamicSchema(){
        return SchemaBuilder.record("User").namespace("cloud.shareApp").fields().requiredString("name").nullableInt("favourite_number",0).endRecord();
    }
}
