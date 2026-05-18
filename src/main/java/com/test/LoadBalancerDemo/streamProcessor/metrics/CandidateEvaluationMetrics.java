package com.test.LoadBalancerDemo.streamProcessor.metrics;

public class CandidateEvaluationMetrics {
    public double cpuUsage;
    public double averageConnectionLatencyToAllUpStreamBolts;
    public double averageConnectionLatencyToAllDownStreamBolts;
    public float remainingInBandwidth;
    public float remainingOutBandwidth;
    public double score;

    @Override
    public String toString() {
        return "CandidateEvaluationMetrics{" +
                "cpuUsage=" + cpuUsage +
                ", averageConnectionLatencyToAllUpStreamBolts=" + averageConnectionLatencyToAllUpStreamBolts +
                ", averageConnectionLatencyToAllDownStreamBolts=" + averageConnectionLatencyToAllDownStreamBolts +
                ", remainingInBandwidth=" + remainingInBandwidth +
                ", remainingOutBandwidth=" + remainingOutBandwidth +
                ", score=" + score +
                '}';
    }

    public float getRemainingOutBandwidth() {
        return remainingOutBandwidth;
    }

    public void setRemainingOutBandwidth(float remainingOutBandwidth) {
        this.remainingOutBandwidth = remainingOutBandwidth;
    }

    public float getRemainingInBandwidth() {
        return remainingInBandwidth;
    }

    public void setRemainingInBandwidth(float remainingInBandwidth) {
        this.remainingInBandwidth = remainingInBandwidth;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getAverageConnectionLatencyToAllUpStreamBolts() {
        return averageConnectionLatencyToAllUpStreamBolts;
    }

    public void setAverageConnectionLatencyToAllUpStreamBolts(double averageConnectionLatencyToAllUpStreamBolts) {
        this.averageConnectionLatencyToAllUpStreamBolts = averageConnectionLatencyToAllUpStreamBolts;
    }

    public double getAverageConnectionLatencyToAllDownStreamBolts() {
        return averageConnectionLatencyToAllDownStreamBolts;
    }

    public void setAverageConnectionLatencyToAllDownStreamBolts(double averageConnectionLatencyToAllDownStreamBolts) {
        this.averageConnectionLatencyToAllDownStreamBolts = averageConnectionLatencyToAllDownStreamBolts;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }


}