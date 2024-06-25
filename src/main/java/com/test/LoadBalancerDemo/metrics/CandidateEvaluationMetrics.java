package com.test.LoadBalancerDemo.metrics;

public class CandidateEvaluationMetrics {
    public double cpuUsage;
    public double averageConnectionLatencyToAllUpStreamBolts;
    public double averageConnectionLatencyToAllDownStreamBolts;
    public double score;


    @Override
    public String toString() {
        return "CandidateEvaluationMetrics{" +
                "cpuUsage=" + cpuUsage +
                ", averageConnectionLatencyToAllUpStreamBolts=" + averageConnectionLatencyToAllUpStreamBolts +
                ", averageConnectionLatencyToAllDownStreamBolts=" + averageConnectionLatencyToAllDownStreamBolts +
                ", score=" + score +
                '}';
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