package kafkaDDLoperations;

import kafkaOperations.KafkaOperationsAssistant;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Component
public class CreateTopic {
    private final AdminClient adminClient;
    private final KafkaOperationsAssistant kafkaOperationsAssistant;

    @Autowired
    public CreateTopic(AdminClient adminClient, KafkaOperationsAssistant kafkaOperationsAssistant) {
        this.adminClient = adminClient;
        this.kafkaOperationsAssistant=kafkaOperationsAssistant;
    }
    public TopicDescription createTopic(String topicName) throws ExecutionException, InterruptedException {
        boolean topicExists = kafkaOperationsAssistant.checkTopicExists(topicName);
        if(!topicExists){
            CreateTopicsResult topics = adminClient.createTopics(List.of(new NewTopic(topicName, Optional.empty(),Optional.empty())));
            topics.all().get();
        }
        return kafkaOperationsAssistant.topicDescription(topicName);
    }
}
