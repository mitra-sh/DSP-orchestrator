package com.test.LoadBalancerDemo.streamProcessor.requests;

public class BoltInfoTestRequest {
    public double cpu;
    public double throughput;
    public String name;

    @Override
    public String toString() {
        return "BoltInfoTestRequest{" +
                "cpu=" + cpu +
                ", throughput=" + throughput +
                ", name='" + name + '\'' +
                '}';
    }
}
