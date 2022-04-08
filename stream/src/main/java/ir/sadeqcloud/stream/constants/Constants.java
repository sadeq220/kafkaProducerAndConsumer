package ir.sadeqcloud.stream.constants;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

/**
 * TODO use @ConfigurationProperties instead of @Component
 */
@Component("constants")
public class Constants {
    private static String stateStoreName;
    private static String stateTopicName;
    public Constants(@Value("${kafka.state.store.name}")String stateStoreName,
                     @Value("${kafka.topic.state.name}")String accumulatedDomainTopicName) {
        Constants.stateStoreName=stateStoreName;
        Constants.stateTopicName=accumulatedDomainTopicName;

    }
    public static String getAccumulatedDomainTopicName(){ return Constants.stateTopicName;}
    public static String getStateStoreName() {
        return Constants.stateStoreName;
    }
}
