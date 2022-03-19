package ir.sadeqcloud.stream;

import org.apache.commons.collections4.list.FixedSizeList;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        return new KafkaStreamsConfiguration(kafkaStreamConfigs);
    }
    @Bean
    /**
     * create our topic
     */
    public NewTopic createTopic(){
        return TopicBuilder.name(outputTopicName).partitions(1).replicas(1).build();
    }
}
