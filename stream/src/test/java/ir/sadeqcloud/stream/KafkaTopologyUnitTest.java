package ir.sadeqcloud.stream;

import ir.sadeqcloud.stream.constants.Constants;
import ir.sadeqcloud.stream.model.BusinessDomain;
import ir.sadeqcloud.stream.service.MyProcessor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * unit test for kafka stream
 * no broker needed
 * Note that there are still some areas where you will need to do integration testing
 * for example, functionality involving pattern subscription or caching.
 */
public class KafkaTopologyUnitTest {

    static private TestInputTopic<String,String> inputTopic;
    static private TestOutputTopic<String, BusinessDomain> outputTopic;
    static private MyProcessor myProcessor;
    /**
     * TopologyTestDriver represents your Kafka Stream application itself
     */
    static private TopologyTestDriver topologyTestDriver;
    static private StreamsBuilder streamsBuilder;


    @BeforeAll
    static void initializeTopologyForTesting(){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"TestStream");

        Serializer<String> serializer = Serdes.String().serializer();
        Deserializer<String> deserializer = Serdes.String().deserializer();

        final String inputTopicName="test";
        final String outPutTopicName="testOut";
        new Constants("testStateStore", "accumulateDomainTopicTest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaTopologyUnitTest.streamsBuilder=streamsBuilder;

        KafkaStreamApplication testStream = new KafkaStreamApplication(List.of(""), "testStream", outPutTopicName);
        KafkaTopologyUnitTest.myProcessor=new MyProcessor(streamsBuilder,
                outPutTopicName,
                null,
                testStream.serializerAndDeserializerForBusinessDomain(),
                testStream.serializerAndDeserializerForDomainAccumulator(),
                null    );
        KafkaTopologyUnitTest.myProcessor.topologyCreation(streamsBuilder,KafkaStreamApplication.buildStoreState());
        Topology topology = streamsBuilder.build();

        KafkaTopologyUnitTest.topologyTestDriver = new TopologyTestDriver(topology,properties);
        KafkaTopologyUnitTest.inputTopic = topologyTestDriver.createInputTopic(inputTopicName, serializer, serializer);



        KafkaTopologyUnitTest.outputTopic=topologyTestDriver.createOutputTopic(outPutTopicName,deserializer,testStream.serializerAndDeserializerForBusinessDomain().deserializer());
    }

    /**
     * you put a message in source topic and read value from output topic
     * and then assert your expectations in output topic
     *
     * Executing TestInputTopic.pipeInput will trigger stream-time punctuation
     * wall-clock punctuations will fire only by calling the advanceWallClockTime method
     */
    @FastUnitTest
    void checkCorrectnessOfEventInOutputTopic(){
        String testingPurposeKey = "TestingPurpose";
        inputTopic.pipeInput(testingPurposeKey,"main1234");
        KeyValue<String, BusinessDomain> stringBusinessDomainKeyValue = outputTopic.readKeyValue();
        Assertions.assertEquals(testingPurposeKey,stringBusinessDomainKeyValue.key);
        Assertions.assertEquals("main",stringBusinessDomainKeyValue.value.getMainPart());
        Assertions.assertEquals(1234,stringBusinessDomainKeyValue.value.getAssociatedNumber());
    }

    @AfterAll
    static void cleanUpResources(){
    topologyTestDriver.close();
    }
}
