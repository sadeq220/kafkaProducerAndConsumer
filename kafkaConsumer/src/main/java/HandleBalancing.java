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
     * this method will invoke on
     * Partition rebalancing
     * consumer client crash( cause by e.g. kill signal)
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
    kafkaConsumer.commitSync(MyKafkaConsumer.OFFSET_TRACKER);
        System.out.println("commit latest offset done");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }
}
