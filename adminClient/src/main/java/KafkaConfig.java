import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.*;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@Import({KafkaTopicBeans.class})
public class KafkaConfig {

    @Bean("kafkaProperties")
    public Properties kafkaProperties(){
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,10_000);//default AdminClient APIs request timeout(120s)
        return properties;
    }
    @Bean
    public AdminClient adminClient(@Qualifier("kafkaProperties") Properties properties){
        return AdminClient.create(properties);
    }
    @Bean
    @Scope(BeanDefinition.SCOPE_PROTOTYPE)
    public Map<String, KafkaFuture<TopicDescription>> interestedTopics(AdminClient adminClient, Collection<String> topicNames){
        return adminClient.describeTopics(topicNames).values();
    }

}
