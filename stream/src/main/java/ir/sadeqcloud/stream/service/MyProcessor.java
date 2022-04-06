package ir.sadeqcloud.stream.service;

import ir.sadeqcloud.stream.constants.Constants;
import ir.sadeqcloud.stream.model.BusinessDomain;
import ir.sadeqcloud.stream.model.BusinessDomainValueTransformer;
import ir.sadeqcloud.stream.model.DomainAccumulator;
import ir.sadeqcloud.stream.utils.FixedPriorityQueueSerde;
import ir.sadeqcloud.stream.utils.FixedSizePriorityQueue;
import jdk.jshell.JShell;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
/**
 * to create your processor topology
 */
public class MyProcessor {
    private StreamsBuilder streamsBuilder;
    private final String outputTopicName;
    private final Serde<BusinessDomain> businessDomainSerde;
    private final Serde<DomainAccumulator> domainAccumulatorSerde;
    private final Serde<String> stringSerde=Serdes.String();

    @Autowired
    public MyProcessor(StreamsBuilder streamsBuilder,
                       @Value("${kafka.streams.output.topic.name}") String outputTopicName,
                       StreamsBuilderFactoryBean factoryBean,
                       @Qualifier("BusinessDomainSerde")Serde<BusinessDomain> businessDomainSerde,
                       @Qualifier("DomainAccumulatorSerde")Serde<DomainAccumulator> domainAccumulatorSerde){
        this.streamsBuilder=streamsBuilder;
        this.outputTopicName=outputTopicName;
        this.businessDomainSerde=businessDomainSerde;
        this.domainAccumulatorSerde=domainAccumulatorSerde;
    }
    @Bean(name = "businessDomainNode")
    /**
     * subscribe to at least one source topic or global table
     * using a high-level DSL
     */
    public KStream<String,BusinessDomain> topologyCreation(StreamsBuilder streamsBuilder, StoreBuilder storeBuilder){
        streamsBuilder.addStateStore(storeBuilder);
        KStream<String, String> sourceNode = streamsBuilder.stream("test", Consumed.with(Serdes.serdeFrom(String.class), Serdes.serdeFrom(String.class)));
        KStream<String, BusinessDomain> processorNode = sourceNode.mapValues((k, v) -> {
            Pattern compile = Pattern.compile("([a-zA-Z]+)([0-9]*)");
            String mainPart=null;
            String associatedNumber=null;
            Matcher matcher = compile.matcher(v);
            if (matcher.find()){
            associatedNumber = matcher.group(2);
            mainPart=matcher.group(1);
            }
            BusinessDomain businessDomain = BusinessDomain.builderFactory().setMainPart(mainPart).setAssociatedNumber(associatedNumber!=null?Long.valueOf(associatedNumber):null).setProcessTime(LocalDateTime.now());
            return businessDomain;
        });
        /**
         * nothing from KStream#peek is forwarded downstream,
         * making it ideal for operations like printing.
         * You can embed it in a chain of processors without the need for a separate print statement.
         */
        processorNode.peek((k,v)->System.out.println(k));
        /**
         * filter-out null messages
         */
        KStream<String, BusinessDomain> filteringProcessorNode = processorNode.filter((k, v) -> v.getMainPart() != null);
        /**
         * sink node .produce new messages to kafka cluster
         */
        filteringProcessorNode.to(outputTopicName, Produced.with(Serdes.String(),businessDomainSerde));
        return filteringProcessorNode;
    }

    /**
     * One quick note about the usage of the processor API in Kafka Streams binder-based applications.
     * The only way you can use the low-level processor API when you use the binder
     * is through a usage pattern of higher-level DSL and then combine that with a transform or process call on it
     */
    @Bean(name = "terminalNode")
    public Void useStateStore(@Qualifier("businessDomainNode")KStream<String,BusinessDomain> kStream){
        kStream.transformValues(()->new BusinessDomainValueTransformer(Constants.getStateStoreName()),Constants.getStateStoreName())
        .to(Constants.getAccumulatedDomainTopicName(),Produced.with(Serdes.String(),domainAccumulatorSerde));
        return null;
    }

    /**
     * Aggregations are key-based operations
     * you can perform aggregations on windowed or non-windowed data
     * Aggregation works in the same manner as KTable
     * Aggregation is a generalization of 'reduce'
     * assume aggregation use of KeyValueStore<GK,VA>
     */
    @Bean(name="aggregator")
    public Void preserveHighAssociatedNumbers(@Qualifier("businessDomainNode")KStream<String,BusinessDomain> kStream){

        kStream.groupBy((k,v)->v.getMainPart())//define group key(GK) & create sub-streams
                /**
                 * aggregates the most recent records with the same key
                 */
                .aggregate(()->new FixedSizePriorityQueue(3,BusinessDomain.class),//initializer , initialize VA ,called per new key(GK) arrive
                (k,v,va)->va.add(v), //adder , define new VA
                Materialized.with(stringSerde,new FixedPriorityQueueSerde()) // materialize state store
                          );
        return null;
    }
}
