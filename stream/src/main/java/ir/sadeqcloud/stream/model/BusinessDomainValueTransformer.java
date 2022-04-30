package ir.sadeqcloud.stream.model;


import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * using the processor API of Kafka Streams
 * @see <a href="https://docs.confluent.io/platform/current/streams/developer-guide/processor-api.html" />
 * this class will used by
 * KStream#transformValues(..)
 * V represents last streaming value type
 * VR represents new streaming value type
 */
public class BusinessDomainValueTransformer implements ValueTransformer<BusinessDomain,DomainAccumulator> {
    private String stateStoreName;
    private KeyValueStore<String,Long> stateStore;
    private ProcessorContext processorContext;

    public BusinessDomainValueTransformer(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
        StateStore stateStore = processorContext.getStateStore(stateStoreName);
        this.stateStore = (KeyValueStore<String, Long>) stateStore;
        /**
         * to schedule a periodic callback - called a punctuation
         *

        processorContext.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(long l) {
            processorContext.forward("recordKey","recordValue");
            }
        });
        */
    }
    @Override
    public DomainAccumulator transform(BusinessDomain businessDomain) {
        DomainAccumulator domainAccumulator = DomainAccumulator.builderFactory(businessDomain);
        // get a "data record" metadata
        long timestamp = processorContext.timestamp();
        LocalDateTime localDateTime = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.ofHoursMinutes(4, 30)).toLocalDateTime();
        //
        if (localDateTime.isAfter(LocalDateTime.now().minusHours(1))) {
            Long accumulatedNumSoFar = stateStore.get(domainAccumulator.getMainPart());
            if (accumulatedNumSoFar != null) {
                domainAccumulator.addAccumulatedAssociatedNumber(accumulatedNumSoFar);
            }
            stateStore.put(domainAccumulator.getMainPart(), domainAccumulator.getAccumulatedAssociatedNumber());
        }
        return domainAccumulator;
    }

    @Override
    public void close() {

    }

}
