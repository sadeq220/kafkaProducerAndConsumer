import kafkaDDLoperations.CreateTopic;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.*;
import org.springframework.lang.Nullable;

import java.util.List;

@Configuration
@ComponentScan({"kafkaDDLoperations","kafkaOperations"})
public class KafkaTopicBeans {
    private AdminClient adminClient;
    @Autowired
    public KafkaTopicBeans(AdminClient adminClient){
        this.adminClient=adminClient;
    }

    /**
     *tell the IoC container to initialize bean lazy
     * we should then use method injection or
     * ObjectProvider<T>
     */
    @Bean(name = "interestedTopic")
    @Lazy
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE) // prototype scope imply lazy initialization
    public KafkaFuture<TopicDescription> interestedTopic(@Nullable String topicName){
        return adminClient.describeTopics(List.of(topicName)).values().get(topicName);
    }
}
