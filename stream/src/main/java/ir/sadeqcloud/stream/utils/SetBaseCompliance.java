package ir.sadeqcloud.stream.utils;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import ir.sadeqcloud.stream.model.BusinessDomain;

import java.io.Serializable;

@JsonTypeInfo(use= JsonTypeInfo.Id.NAME,property = "class")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BusinessDomain.class,name = "businessDomain"),
        @JsonSubTypes.Type(value = BusinessDomain.class,name = "simple")// backward compatibility
})
public interface SetBaseCompliance extends Comparable, Serializable {
}
