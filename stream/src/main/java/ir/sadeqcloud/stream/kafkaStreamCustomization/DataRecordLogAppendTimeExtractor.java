package ir.sadeqcloud.stream.kafkaStreamCustomization;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Kafka Streams assigns a timestamp to every data record via so-called timestamp extractors
 * TimestampExtractor is used to create a SourceNodeFactory, RecordQueue .
 * RecordQueue acts as a buffer of Kafka ConsumerRecords.
 * RecordQueue is created along with a StreamTask exclusively (for every partition assigned).
 * Finally, whenever a Kafka Streams application writes records to Kafka, then it will also assign timestamps to these new records.(defaults to current time in UTC )
 */
public class DataRecordLogAppendTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        return consumerRecord.timestamp();
    }
}
