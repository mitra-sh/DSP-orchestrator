package com.test.LoadBalancerDemo.streamProcessor.metrics;

public class RemovalCandidate {
    public double remainingInBW;
    public double remainingOutBW;
    public double cpu;
    public double score;

    public RemovalCandidate(double remainingInBW, double getRemainingOutBW, double cpu) {
        this.remainingInBW = remainingInBW;
        this.remainingOutBW = getRemainingOutBW;
        this.cpu = cpu;
    }

    @Override
    public String toString() {
        return "RemovalCandidate{" +
                "cpu=" + cpu +
                ", remainingOutBW=" + remainingOutBW +
                ", remainingInBW=" + remainingInBW +
                '}';
    }

    public double getRemainingInBW() {
        return remainingInBW;
    }

    public void setRemainingInBW(double remainingInBW) {
        this.remainingInBW = remainingInBW;
    }

    public double getRemainingOutBW() {
        return remainingOutBW;
    }

    public void setRemainingOutBW(double remainingOutBW) {
        this.remainingOutBW = remainingOutBW;
    }

    public double getCpu() {
        return cpu;
    }

    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
