package ir.sadeqcloud.stream.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class FixedPriorityQueueSerde extends Serdes.WrapperSerde<FixedSizePriorityQueue> {
    public FixedPriorityQueueSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(FixedSizePriorityQueue.class));
    }
}
