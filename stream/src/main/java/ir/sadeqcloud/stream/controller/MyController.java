package ir.sadeqcloud.stream.controller;

import ir.sadeqcloud.stream.utils.IoCContainerUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RestController
public class MyController {
    /**
     * to access kafka state store
     */
    private final StreamsBuilderFactoryBean streamsBuilderFactory;
    @Autowired
    public MyController(StreamsBuilderFactoryBean streamsBuilderFactory){
        this.streamsBuilderFactory=streamsBuilderFactory;
    }
    @GetMapping("/")
    public ResponseEntity home(@RequestParam(name = "main") String mainPart){
        KafkaStreams kafkaStreams = streamsBuilderFactory.getKafkaStreams();
        ReadOnlyKeyValueStore<Object, Object> highNumbersStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType("highDomainsStore", QueryableStoreTypes.keyValueStore()));
        return ResponseEntity.ok(highNumbersStore.get(mainPart));
    }
    @GetMapping("/countEvents")
    public ResponseEntity countedEvents(@RequestParam(name = "main") String mainPart){
        KafkaStreams kafkaStreams = streamsBuilderFactory.getKafkaStreams();
        String countStore = IoCContainerUtil.getBean(String.class, "windowedAggregator");
        ReadOnlyWindowStore<Object, Object> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(countStore, QueryableStoreTypes.windowStore()));
        return ResponseEntity.ok(store.fetch(mainPart,1));
    }
}
