package com.test.LoadBalancerDemo;

public class MessageProcessingTimeUnit {
    public Long startingTime;
    public Long endTime;
    public Long executionTime;
    public MessageProcessingTimeUnit(Long startingTime) {
        this.startingTime = startingTime;
    }
    public MessageProcessingTimeUnit() {

    }


    @Override
    public String toString() {
        return "startingTime=" + startingTime +
                ", executionTime=" + executionTime+" ms";
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Long getStartingTime() {
        return startingTime;
    }

    public void setStartingTime(Long startingTime) {
        this.startingTime = startingTime;
    }

    public Long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(Long executionTime) {
        this.executionTime = executionTime;
    }

}
