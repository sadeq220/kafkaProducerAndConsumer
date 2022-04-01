package kafkaDDLoperations;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class DeleteTopic {
    private final AdminClient adminClient;
    @Autowired
    public DeleteTopic(AdminClient adminClient) {
        this.adminClient = adminClient;
    }
    public boolean deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        boolean topicExists = checkTopicExists(topicName);
        if (!topicExists){
            return false;
        }
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(List.of(topicName));
        deleteTopicsResult.all().get();
        return true;
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
