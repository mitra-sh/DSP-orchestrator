package com.test.LoadBalancerDemo.streamProcessor;

public class MessageProcessingTimeUnit {
    public Long startingTime;
    public Long endTime;
    public int emitRate;
    public long cycle;
    public MessageProcessingTimeUnit(Long startingTime,long cycle,int emitRate) {
        this.startingTime = startingTime;
        this.cycle = cycle;
        this.emitRate=emitRate;
    }
    public MessageProcessingTimeUnit() {

    }


    @Override
    public String toString() {
        return "startingTime=" + startingTime +" ms";
    }

    public long getCycle() {
        return cycle;
    }

    public void setCycle(long cycle) {
        this.cycle = cycle;
    }

    public int getEmitRate() {
        return emitRate;
    }

    public void setEmitRate(int emitRate) {
        this.emitRate = emitRate;
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



}
