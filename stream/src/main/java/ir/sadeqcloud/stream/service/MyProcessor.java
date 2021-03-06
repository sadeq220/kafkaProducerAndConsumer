package ir.sadeqcloud.stream.service;

import ir.sadeqcloud.stream.constants.Constants;
import ir.sadeqcloud.stream.model.BusinessDomain;
import ir.sadeqcloud.stream.model.BusinessDomainValueTransformer;
import ir.sadeqcloud.stream.model.DomainAccumulator;
import ir.sadeqcloud.stream.utils.FixedSizePriorityQueue;
import jdk.jshell.JShell;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
/**
 * to create your processor topology(with high-level DSL)
 * <a href="https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html" />
 */
public class MyProcessor {
    private StreamsBuilder streamsBuilder;
    private final String outputTopicName;
    private final Serde<BusinessDomain> businessDomainSerde;
    private final Serde<DomainAccumulator> domainAccumulatorSerde;
    private final Serde<String> stringSerde=Serdes.String();
    private final Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde;

    @Autowired
    public MyProcessor(StreamsBuilder streamsBuilder,
                       @Value("${kafka.streams.output.topic.name}") String outputTopicName,
                       StreamsBuilderFactoryBean factoryBean,
                       @Qualifier("BusinessDomainSerde")Serde<BusinessDomain> businessDomainSerde,
                       @Qualifier("DomainAccumulatorSerde")Serde<DomainAccumulator> domainAccumulatorSerde,
                       @Qualifier("FixedSizePriorityQueueSerde") Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde){
        this.streamsBuilder=streamsBuilder;
        this.outputTopicName=outputTopicName;
        this.businessDomainSerde=businessDomainSerde;
        this.domainAccumulatorSerde=domainAccumulatorSerde;
        this.fixedSizePriorityQueueSerde=fixedSizePriorityQueueSerde;
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
    @Bean(name="rollingAggregator")
    public Void preserveHighAssociatedNumbers(@Qualifier("businessDomainNode")KStream<String,BusinessDomain> kStream){
        Materialized<String, FixedSizePriorityQueue, KeyValueStore<Bytes,byte[]>> highDomainsStore = Materialized.as("highDomainsStore");
        highDomainsStore.withKeySerde(stringSerde);
        highDomainsStore.withValueSerde(fixedSizePriorityQueueSerde);
        /**
         * groupBy always causes data 're-partitioning'
         * This redistribution stage, usually called 'data shuffling'
         */
        kStream.groupBy((k,v)->v.getMainPart(),//define group key(GK) & create sub-streams/sub-topology
                        Grouped.with(stringSerde,businessDomainSerde))
                /**
                 * aggregates the most recent records with the same key
                 */
                .aggregate(()->new FixedSizePriorityQueue<BusinessDomain>(3),//initializer , initialize VA ,called per new key(GK) arrive
                (k,v,va)->va.add(v), //adder , define new VA
                highDomainsStore // materialize state store
                          );
        return null;
    }
    @Bean("windowedAggregator")
    /**
     * method "preserveHighAssociatedNumbers" define rolling aggregation , here we want to define Windowed aggregation
     * windowing allows us to take snapshot of aggregation over a given timeframe
     * there are four types of windows, here we used 'Hopping window'
     * assume windowed aggregation use of WindowStore<Windowed<GK>, VA>
     */
    public String retentionOfAssociatedNumbersOverTimeFrame(@Qualifier("businessDomainNode")KStream<String,BusinessDomain> kStream){
        Materialized<String,Long, WindowStore<Bytes,byte[]>> countByMainPart = Materialized.as("countByMainPart");
        countByMainPart.withKeySerde(stringSerde);
        countByMainPart.withValueSerde(Serdes.Long());

        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))//no Grace means if "event time" < "stream time" , then record ignored
                .advanceBy(Duration.ofMinutes(1));//hopping window ( advance < size )

        KTable<Windowed<String>, Long> count = kStream.groupBy((k, v) -> v.getMainPart(), Grouped.with(stringSerde, businessDomainSerde)).windowedBy(timeWindows).count(countByMainPart);
        return count.queryableStoreName();
    }
}
