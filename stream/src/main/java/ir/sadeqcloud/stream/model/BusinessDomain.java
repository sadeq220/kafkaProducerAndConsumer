package ir.sadeqcloud.stream.model;

import ir.sadeqcloud.stream.utils.SetBaseCompliance;

import java.time.LocalDateTime;

public class BusinessDomain implements SetBaseCompliance {
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

    @Override
    /**
     * used in TreeSet
     */
    public int compareTo(Object o) {
        if (!(o instanceof BusinessDomain))
            throw new IllegalArgumentException(o.getClass().toString()+"is not type of"+this.getClass().toString());
        BusinessDomain businessDomain = (BusinessDomain) o;
        return businessDomain.associatedNumber.compareTo(associatedNumber);
    }
}
