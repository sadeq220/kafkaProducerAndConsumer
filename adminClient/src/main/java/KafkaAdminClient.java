import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * we want to manage topics with AdminClient
 * Each method returns immediately after delivering a request to the
 * cluster Controller, and each method returns one or more Future objects
 */
public class KafkaAdminClient {
    private final static AdminClient adminClient;
    static {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,10_000);//default AdminClient APIs request timeout(120s)

        adminClient=AdminClient.create(properties);//it will create daemon thread
    }
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String topicName=args.length > 0 ? args[0] : "test";
        // topicName= ${1:-test}

        DescribeTopicsResult topicTest = adminClient.describeTopics(List.of(topicName));
        try{
            TopicDescription topicDescription = topicTest.values().get(topicName).get();
            String result = String.format("topic called %s exists with %d partitions", topicName, topicDescription.partitions().size());
            System.out.println(result);
        }catch (ExecutionException e){
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)){
           e.printStackTrace();
           System.exit(1);
            }
            CreateTopicsResult topics = adminClient.createTopics(List.of(new NewTopic(topicName, Optional.empty(),Optional.empty())));
            /**
             * controller will send LeaderAndISR and UpdateMetadata api_keys to any broker in the cluster
             * to notify them about new partition and leader of it , brokers also update their MetadataCache
             *
             * -note ISR stands for "In-Sync Replica's"
             */
            String result = String.format("topic named %s created with %d partitions", topicName, topics.numPartitions(topicName).get());
            System.out.println(result);
        }

    }
}
