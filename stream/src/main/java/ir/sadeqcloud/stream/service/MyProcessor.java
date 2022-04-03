package ir.sadeqcloud.stream.service;

import ir.sadeqcloud.stream.model.BusinessDomain;
import jdk.jshell.JShell;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
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

    @Autowired
    public MyProcessor(StreamsBuilder streamsBuilder,
                       @Value("${kafka.streams.output.topic.name}") String outputTopicName,
                       StreamsBuilderFactoryBean factoryBean,
                       @Qualifier("BusinessDomainSerde")Serde<BusinessDomain> businessDomainSerde){
        this.streamsBuilder=streamsBuilder;
        this.outputTopicName=outputTopicName;
        this.businessDomainSerde=businessDomainSerde;
    }
    @Bean
    /**
     * subscribe to at least one source topic or global table
     */
    public KStream<String,String> topologyCreation(StreamsBuilder streamsBuilder){
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
            BusinessDomain businessDomain = BusinessDomain.builderFactory().setMainPart(mainPart).setAssociatedNumber(Long.valueOf(associatedNumber)).setProcessTime(LocalDateTime.now());
            return businessDomain;
        });
        /**
         * nothing from KStream#peek is forwarded downstream,
         * making it ideal for operations like printing.
         * You can embed it in a chain of processors without the need for a separate print statement.
         */
        processorNode.peek((k,v)->System.out.println(k));
        processorNode.to(outputTopicName, Produced.with(Serdes.String(),businessDomainSerde));
        return sourceNode;
    }
}
