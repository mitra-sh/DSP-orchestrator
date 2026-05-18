package com.test.LoadBalancerDemo.streamProcessor.metrics;

public class TransferLatency {
    public String nameOfDestinationBolt;
    public double latency;

    public String getNameOfDestinationBolt() {
        return nameOfDestinationBolt;
    }
    public void setNameOfDestinationBolt(String nameOfDestinationBolt) {
        this.nameOfDestinationBolt = nameOfDestinationBolt;
    }
    public void setLatency(double latency) {
        this.latency = latency;
    }
    public double getLatency() {
        return latency;
    }


}
