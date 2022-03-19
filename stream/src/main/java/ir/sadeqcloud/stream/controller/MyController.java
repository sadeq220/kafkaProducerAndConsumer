package ir.sadeqcloud.stream.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

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
    public ResponseEntity home(){
        KafkaStreams kafkaStreams = streamsBuilderFactory.getKafkaStreams();
        return ResponseEntity.ok(null);
    }
}
