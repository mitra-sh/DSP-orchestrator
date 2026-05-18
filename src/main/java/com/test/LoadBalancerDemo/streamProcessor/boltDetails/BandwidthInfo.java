package com.test.LoadBalancerDemo.streamProcessor.boltDetails;

public class BandwidthInfo {
    public float inBandwidth;
    public float outBandwidth;

    public BandwidthInfo(float inBandwidth, float outBandwidth) {
        this.inBandwidth = inBandwidth;
        this.outBandwidth = outBandwidth;
    }

    @Override
    public String toString() {
        return "{" +
                "inBandwidth=" + inBandwidth +
                ", outBandwidth=" + outBandwidth +
                '}';
    }

    public float getInBandwidth() {
        return inBandwidth;
    }

    public void setInBandwidth(float inBandwidth) {
        this.inBandwidth = inBandwidth;
    }

    public float getOutBandwidth() {
        return outBandwidth;
    }

    public void setOutBandwidth(float outBandwidth) {
        this.outBandwidth = outBandwidth;
    }
}
