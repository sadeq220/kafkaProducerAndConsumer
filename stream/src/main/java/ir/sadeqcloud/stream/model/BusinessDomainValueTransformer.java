package ir.sadeqcloud.stream.model;


import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

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
    }
    @Override
    public DomainAccumulator transform(BusinessDomain businessDomain) {
        DomainAccumulator domainAccumulator = DomainAccumulator.builderFactory(businessDomain);
        Long accumulatedNumSoFar = stateStore.get(domainAccumulator.getMainPart());
        if (accumulatedNumSoFar!=null){
        domainAccumulator.addAccumulatedAssociatedNumber(accumulatedNumSoFar);
        }
        stateStore.put(domainAccumulator.getMainPart(),domainAccumulator.getAccumulatedAssociatedNumber());
        return domainAccumulator;
    }

    @Override
    public void close() {

    }
}
