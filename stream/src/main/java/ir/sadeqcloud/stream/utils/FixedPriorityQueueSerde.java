package ir.sadeqcloud.stream.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

public class FixedPriorityQueueSerde extends WrapperSerde<FixedSizePriorityQueue> {
    public FixedPriorityQueueSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(FixedSizePriorityQueue.class));
    }
}
class WrapperSerde<T> implements Serde<T> {

    private JsonSerializer<T> serializer;
    private JsonDeserializer<T> deserializer;

    WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
