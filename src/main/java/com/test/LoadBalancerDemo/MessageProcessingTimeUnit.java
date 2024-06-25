package com.test.LoadBalancerDemo;

public class MessageProcessingTimeUnit {
    public long startingTime;
    public long executionTime;

    @Override
    public String toString() {
        return "startingTime=" + startingTime +
                ", executionTime=" + executionTime+" ms";
    }

    public MessageProcessingTimeUnit(long startingTime) {
        this.startingTime = startingTime;
    }

    public long getStartingTime() {
        return startingTime;
    }

    public void setStartingTime(long startingTime) {
        this.startingTime = startingTime;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

}
