package ir.sadeqcloud.stream.springEventListener;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
/**
 * use link blow to analyze your topology
 * <a href="https://zz85.github.io/kafka-streams-viz/" />
 */
public class ApplicationReadyListener implements ApplicationListener<ApplicationReadyEvent> {
    @Autowired
    private StreamsBuilder streamsBuilder;
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
    System.out.println("topology analysis");
        Topology topology = streamsBuilder.build();
        System.out.println(topology.describe());
    }
}
