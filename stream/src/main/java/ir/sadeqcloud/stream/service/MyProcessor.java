package ir.sadeqcloud.stream.service;

import jdk.jshell.JShell;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
/**
 * to create your processor topology
 */
public class MyProcessor {
    private StreamsBuilder streamsBuilder;
    private final String outputTopicName;

    @Autowired
    public MyProcessor(StreamsBuilder streamsBuilder,
                       @Value("${kafka.streams.output.topic.name}") String outputTopicName,
                       StreamsBuilderFactoryBean factoryBean){
        this.streamsBuilder=streamsBuilder;
        this.outputTopicName=outputTopicName;
    }
    @Bean
    /**
     * subscribe to at least one source topic or global table
     */
    public KStream<String,String> topologyCreation(StreamsBuilder streamsBuilder){
        KStream<String, String> sourceNode = streamsBuilder.stream("test", Consumed.with(Serdes.serdeFrom(String.class), Serdes.serdeFrom(String.class)));
        KStream<String, String> processorNode = sourceNode.mapValues((k, v) -> {
            Pattern compile = Pattern.compile("([a-zA-Z]+)([0-9]*)");
            Matcher matcher = compile.matcher(v);
            if (matcher.find()){
            String group = matcher.group(2);
            return group;}
            return null;
        });
        processorNode.to(outputTopicName, Produced.with(Serdes.String(),Serdes.String()));
        return sourceNode;
    }
}
