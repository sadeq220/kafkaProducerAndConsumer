package ir.sadeqcloud.stream.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class FixedSizePriorityQueueSerializer implements Serializer<FixedSizePriorityQueue<SetBaseCompliance>> {

    private final ObjectMapper objectMapper;

    @Autowired
    public FixedSizePriorityQueueSerializer(ObjectMapper objectMapper){
    this.objectMapper=objectMapper;
    }
    public FixedSizePriorityQueueSerializer(){
        objectMapper=IoCContainerUtil.getBean(ObjectMapper.class);
    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, FixedSizePriorityQueue<SetBaseCompliance> tFixedSizePriorityQueue) {
        byte[] bytes=null;
        if (tFixedSizePriorityQueue != null) {
            try {
                bytes = objectMapper.writeValueAsBytes(tFixedSizePriorityQueue);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, FixedSizePriorityQueue<SetBaseCompliance> data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
