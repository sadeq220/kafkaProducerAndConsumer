package ir.sadeqcloud.stream.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
@Component
public class FixedSizePriorityQueueDeserializer implements Deserializer<FixedSizePriorityQueue<SetBaseCompliance>> {

    private final ObjectMapper objectMapper;
    @Autowired
    public FixedSizePriorityQueueDeserializer(ObjectMapper objectMapper) {
        this.objectMapper=objectMapper;
    }

    public FixedSizePriorityQueueDeserializer(){
        objectMapper=IoCContainerUtil.getBean(ObjectMapper.class);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public FixedSizePriorityQueue<SetBaseCompliance> deserialize(String s, byte[] bytes) {
        FixedSizePriorityQueue<SetBaseCompliance> fixedSizePriorityQueue=null;
        try {
            JsonNode jsonNode = objectMapper.readTree(bytes);
            JsonNode domains = jsonNode.path("Domains");
            if (domains.isArray()){
                ArrayNode domainArray=(ArrayNode) domains;
                FixedSizePriorityQueue<SetBaseCompliance> deserializedResult = new FixedSizePriorityQueue<>(domainArray.size());
                for (int i=0;i<domainArray.size();i++){
                    JsonNode jn = domainArray.get(i);
                    SetBaseCompliance deserializerBaseUtil = objectMapper.treeToValue(jn, SetBaseCompliance.class);
                    deserializedResult.add(deserializerBaseUtil);
                }
                fixedSizePriorityQueue=deserializedResult;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return fixedSizePriorityQueue;
    }

    @Override
    public FixedSizePriorityQueue<SetBaseCompliance> deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
