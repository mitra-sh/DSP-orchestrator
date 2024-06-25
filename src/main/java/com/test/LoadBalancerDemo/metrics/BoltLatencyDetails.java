package com.test.LoadBalancerDemo.metrics;

public class BoltLatencyDetails {
    //nae of the neighbour
    public String boltName;
    public double connectionLatencyToThisBolt;


    @Override
    public String toString() {
        return "{" +
                "boltName='" + boltName + '\'' +
                ", connectionLatencyToThisBolt=" + connectionLatencyToThisBolt +
                '}';
    }

    public double getConnectionLatencyToThisBolt() {
        return connectionLatencyToThisBolt;
    }

    public void setConnectionLatencyToThisBolt(double connectionLatencyToThisBolt) {
        this.connectionLatencyToThisBolt = connectionLatencyToThisBolt;
    }

    public String getBoltName() {
        return boltName;
    }

    public void setBoltName(String boltName) {
        this.boltName = boltName;
    }
}
