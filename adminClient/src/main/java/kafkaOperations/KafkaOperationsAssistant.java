package kafkaOperations;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
public class KafkaOperationsAssistant {

    public TopicDescription topicDescription(String topicName){
        try {
            return this.getTopicDescription(topicName).get();
        } catch (InterruptedException | ExecutionException e){
            e.printStackTrace();
            throw new RuntimeException(e.getCause());
        }
    }
    public boolean checkTopicExists(String topicName){
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
