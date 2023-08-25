import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class HandleBalancing implements ConsumerRebalanceListener {
    private final KafkaConsumer kafkaConsumer;

    public HandleBalancing(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * Called when the consumer has to give up partitions that it previously owned -
     * either as a result of a rebalance or when the consumer is being closed.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
    kafkaConsumer.commitSync(MyKafkaConsumer.OFFSET_TRACKER);
        System.out.println("commit latest offset done");
    }

    /**
     * in cooperative rebalance, this method will be invoked on every rebalance.
     * as a way of notifying the consumer that a rebalance happened.
     * However, if there are no new partitions assigned to the consumer,
     * it will be called with an empty collection.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        /**
         * If offsets are stored in persistent storage other than kafka then,
         *
         * kafkaConsumer.seek(topicPartition,offset);
         *
         * should be called here to continue from last processed event
         */
    }
}
