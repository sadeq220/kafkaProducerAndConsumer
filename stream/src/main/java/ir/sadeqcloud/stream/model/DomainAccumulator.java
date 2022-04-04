package ir.sadeqcloud.stream.model;


import java.util.Timer;

public class DomainAccumulator {
    private String mainPart;
    private Long accumulatedAssociatedNumber;

    public String getMainPart() {
        return mainPart;
    }

    public void setMainPart(String mainPart) {
        this.mainPart = mainPart;
    }

    public Long getAccumulatedAssociatedNumber() {
        return accumulatedAssociatedNumber;
    }

    public void setAccumulatedAssociatedNumber(Long accumulatedAssociatedNumber) {
        this.accumulatedAssociatedNumber = accumulatedAssociatedNumber;
    }

    public void addAccumulatedAssociatedNumber(Long accumulatedAssociatedNumber){
    this.accumulatedAssociatedNumber+=accumulatedAssociatedNumber;
    }
    public static DomainAccumulator builderFactory(BusinessDomain businessDomain){
        DomainAccumulator domainAccumulator = new DomainAccumulator();
        domainAccumulator.mainPart=businessDomain.getMainPart();
        domainAccumulator.accumulatedAssociatedNumber= businessDomain.getAssociatedNumber();
        return domainAccumulator;
    }
}
