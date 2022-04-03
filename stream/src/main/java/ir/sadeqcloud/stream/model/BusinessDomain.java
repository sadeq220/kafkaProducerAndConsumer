package ir.sadeqcloud.stream.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class BusinessDomain implements Serializable {
    private String mainPart;
    private Long associatedNumber;
    private LocalDateTime processTime;

    public String getMainPart() {
        return mainPart;
    }

    public BusinessDomain setMainPart(String mainPart) {
        this.mainPart = mainPart;
        return this;
    }

    public Long getAssociatedNumber() {
        return associatedNumber;
    }

    public BusinessDomain setAssociatedNumber(Long associatedNumber) {
        this.associatedNumber = associatedNumber;
        return this;
    }

    public LocalDateTime getProcessTime() {
        return processTime;
    }

    public BusinessDomain setProcessTime(LocalDateTime processTime) {
        this.processTime = processTime;
        return this;
    }

    public static BusinessDomain builderFactory(){
        return new BusinessDomain();
    }

    @Override
    /**
     * KStream#print() will use this method to print out processing node data
     */
    public String toString() {
        return super.toString();
    }
}
