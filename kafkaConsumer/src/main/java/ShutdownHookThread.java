import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ShutdownHookThread extends Thread{
    private KafkaConsumer kafkaConsumer;
    private Thread mainThread;
    public ShutdownHookThread(Thread mainThread, KafkaConsumer kafkaConsumer){
        super();
        this.kafkaConsumer=kafkaConsumer;
        this.mainThread=mainThread;
    }
    @Override
    public void run() {
        // WakeupException on poll(duration) call
        kafkaConsumer.wakeup();
        try {
            mainThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
