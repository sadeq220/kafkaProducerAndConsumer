package kafkaDDLoperations;

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

    @Autowired
    public CreateTopic(AdminClient adminClient) {
        this.adminClient = adminClient;
    }
    public TopicDescription createTopic(String topicName) throws ExecutionException, InterruptedException {
        boolean topicExists = checkTopicExists(topicName);
        if(!topicExists){
            CreateTopicsResult topics = adminClient.createTopics(List.of(new NewTopic(topicName, Optional.empty(),Optional.empty())));
            topics.all().get();
        }
        return getTopicDescription(topicName).get();
    }
    private boolean checkTopicExists(String topicName){
        try {
            TopicDescription topicDescription = getTopicDescription(topicName).get();
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException)
                return false;
          e.printStackTrace();
          throw new RuntimeException(e.getMessage());
        }
    }

    /**
     *this Method is a target for CGLIB library
     * hence it can either have 'protected' or 'public' access modifiers
     */
    @Lookup("interestedTopic")
    protected KafkaFuture<TopicDescription> getTopicDescription(String topicName){
        return null;
    }
}
