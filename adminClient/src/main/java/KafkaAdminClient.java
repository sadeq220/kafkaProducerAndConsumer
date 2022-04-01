import kafkaDDLoperations.CreateTopic;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * we want to manage topics with AdminClient
 * Each method returns immediately after delivering a request to the
 * cluster Controller, and each method returns one or more Future objects(KafkaFuture<T>)
 */
public class KafkaAdminClient {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String topicName=args.length > 0 ? args[0] : "test";
        // topicName= ${1:-test}
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(KafkaConfig.class);
        AdminClient adminClient = applicationContext.getBean(AdminClient.class);
        DescribeTopicsResult topicTest = adminClient.describeTopics(List.of(topicName));
        try{
            TopicDescription topicDescription = topicTest.values().get(topicName).get();
            String result = String.format("topic named %s exists with %d partitions", topicName, topicDescription.partitions().size());
            System.out.println(result);
        }catch (ExecutionException e){
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)){
           e.printStackTrace();
           System.exit(1);
            }
            CreateTopic crTopic = applicationContext.getBean(CreateTopic.class);
       //     CreateTopicsResult topics = adminClient.createTopics(List.of(new NewTopic(topicName, Optional.empty(),Optional.empty())));
            TopicDescription topicDescription = crTopic.createTopic(topicName);
            /**
             * controller will send LeaderAndISR and UpdateMetadata api_keys to any broker in the cluster
             * to notify them about new partitions and leaders of them , brokers also update their MetadataCache
             *
             * -note ISR stands for "In-Sync Replica's"
             */
            String result = String.format("topic named %s created with %d partitions", topicName, topicDescription.partitions().size());
            System.out.println(result);
        }

    }
}
