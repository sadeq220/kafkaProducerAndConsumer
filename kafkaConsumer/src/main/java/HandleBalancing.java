import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class HandleBalancing implements ConsumerRebalanceListener {
    private KafkaConsumer kafkaConsumer;

    public HandleBalancing(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
    kafkaConsumer.commitSync(MyKafkaConsumer.CONCURRENT_HASH_MAP);
        System.out.println("commit latest offset done");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }
}
