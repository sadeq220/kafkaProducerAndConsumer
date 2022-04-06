package ir.sadeqcloud.stream;

import ir.sadeqcloud.stream.constants.Constants;
import ir.sadeqcloud.stream.model.BusinessDomain;
import ir.sadeqcloud.stream.model.DomainAccumulator;
import org.apache.commons.collections4.list.FixedSizeList;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * https://docs.spring.io/spring-kafka/docs/2.6.2/reference/html/#streams-spring
 */
@SpringBootApplication
@Import({KafkaProducerConfiguration.class})
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamApplication {
    private final List<String> brokers;
    private final String applicationId;
    private final String outputTopicName;
    public KafkaStreamApplication(@Value("${spring.kafka.bootstrap-servers}") List<String> brokers,
                                  @Value("${spring.application.name}") String applicationId,
                                  @Value("${kafka.streams.output.topic.name}") String outputTopicName){
        this.applicationId=applicationId;
        this.outputTopicName=outputTopicName;
        this.brokers= Collections.unmodifiableList(brokers);
    }
    @DependsOn({"constants"})
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamApplication.class, args);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration(){
        HashMap<String, Object> kafkaStreamConfigs = new HashMap<>();
        kafkaStreamConfigs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);//mandatory
        kafkaStreamConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG,applicationId);//mandatory ,  Each stream processing application must have a unique ID. The same ID must be given to all instances of the application
        /**
         * This ID is used in the following places to isolate resources used by the application from others:
         *
         *     As the default Kafka consumer and producer client.id prefix
         *     As the Kafka consumer group.id for coordination
         *     As the name of the subdirectory in the state directory (cf. state.dir)
         *     As the prefix of internal Kafka topic names
         */
        kafkaStreamConfigs.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,10_000);//define KTable cache interval flush
        kafkaStreamConfigs.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,1);//default one , use more when subscribing to 2 or more partitions ,for parallelism.
        return new KafkaStreamsConfiguration(kafkaStreamConfigs);
    }

    /**
     * By default, Kafka Streams assumes data type <byte[],byte[]>
     */
    @Bean(name = "BusinessDomainSerde")
    public Serde<BusinessDomain> serializerAndDeserializerForBusinessDomain(){
        JsonSerializer<BusinessDomain> kafkaJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<BusinessDomain> kafkaJsonDeserializer=new JsonDeserializer<>(BusinessDomain.class);
        Serde<BusinessDomain> businessDomainSerde = Serdes.serdeFrom(kafkaJsonSerializer, kafkaJsonDeserializer);
        return businessDomainSerde;
    }
    @Bean(name = "DomainAccumulatorSerde")
    public Serde<DomainAccumulator> serializerAndDeserializerForDomainAccumulator(){
        JsonSerializer<DomainAccumulator> kafkaJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<DomainAccumulator> kafkaJsonDeserializer=new JsonDeserializer<>(DomainAccumulator.class);
        Serde<DomainAccumulator> businessDomainSerde = Serdes.serdeFrom(kafkaJsonSerializer, kafkaJsonDeserializer);
        return businessDomainSerde;
    }
    /**
    * adding a in-memory state store to our topology
    * kafka stream binder create state stores and pass them along with
    * the underlying StreamsBuilder through the StreamsBuilderFactoryBean ; TODO : StreamsBuilderFactoryBean infrastructureCustomizer -this allows customization of the StreamsBuilder (e.g. to add a state store) and/or the Topology before the stream is created
    */
    @Bean
    public static StoreBuilder buildStoreState(){
        /**
         * All the StateStoreSupplier types have logging enabled by default
         * By default, Kafka Streams creates changelog topics with a delete policy of compact .
         * In a changelog, each incoming record overwrites the previous one with the same key.
         */
        KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(Constants.getStateStoreName());
        return Stores.keyValueStoreBuilder(keyValueBytesStoreSupplier,Serdes.String(),Serdes.Long());
    }
    @Bean
    /**
     * create our topic
     */
    public NewTopic createTopic(){
        return TopicBuilder.name(outputTopicName).partitions(1).replicas(1).build();
    }

    @Bean("stateTopic")
    public NewTopic createTopicTesting(){
        return TopicBuilder.name(Constants.getAccumulatedDomainTopicName()).partitions(1).replicas(1).build();
    }
}
