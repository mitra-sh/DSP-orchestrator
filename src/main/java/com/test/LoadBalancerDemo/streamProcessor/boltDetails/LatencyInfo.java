package com.test.LoadBalancerDemo.streamProcessor.boltDetails;

public class LatencyInfo {
    public String source;
    public String dest;
    public double latency;

    public LatencyInfo(String source, String dest, double latency) {
        this.source = source;
        this.dest = dest;
        this.latency = latency;
    }

    @Override
    public String toString() {
        return "LatencyInfo{" +
                "source='" + source + '\'' +
                ", dest='" + dest + '\'' +
                ", latency=" + latency +
                '}';
    }
}
