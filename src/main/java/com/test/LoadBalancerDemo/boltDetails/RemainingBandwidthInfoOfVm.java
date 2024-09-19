package com.test.LoadBalancerDemo.boltDetails;

public class RemainingBandwidthInfoOfVm {
    public float remainingInBandwidth;
    public float remainingOutBandwidth;
    public float remainingInBandwidthPercentage;
    public float remainingOutBandwidthPercentage;
    public RemainingBandwidthInfoOfVm() {
    }

    public float getRemainingInBandwidth() {
        return remainingInBandwidth;
    }

    public float getRemainingInBandwidthPercentage() {
        return remainingInBandwidthPercentage;
    }

    public void setRemainingInBandwidthPercentage(float remainingInBandwidthPercentage) {
        this.remainingInBandwidthPercentage = remainingInBandwidthPercentage;
    }

    public float getRemainingOutBandwidthPercentage() {
        return remainingOutBandwidthPercentage;
    }

    public void setRemainingOutBandwidthPercentage(float remainingOutBandwidthPercentage) {
        this.remainingOutBandwidthPercentage = remainingOutBandwidthPercentage;
    }

    public void setRemainingInBandwidth(float remainingInBandwidth) {
        this.remainingInBandwidth = remainingInBandwidth;
    }

    public float getRemainingOutBandwidth() {
        return remainingOutBandwidth;
    }

    public void setRemainingOutBandwidth(float remainingOutBandwidth) {
        this.remainingOutBandwidth = remainingOutBandwidth;
    }

    @Override
    public String toString() {
        return "{" +
                "remainingInBandwidth=" + remainingInBandwidth +
                ", remainingOutBandwidth=" + remainingOutBandwidth +
                '}';
    }
}
