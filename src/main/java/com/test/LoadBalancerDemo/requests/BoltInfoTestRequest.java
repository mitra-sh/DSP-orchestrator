package com.test.LoadBalancerDemo.requests;

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
