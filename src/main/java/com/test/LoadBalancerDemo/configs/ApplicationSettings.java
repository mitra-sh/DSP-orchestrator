package com.test.LoadBalancerDemo.configs;

import java.io.Serializable;

public class ApplicationSettings implements Serializable {
    public String mood;
    public int metric_collection_interval_in_second;
    //done
    public double max_acceptable_latency_in_second;
    //done
    public double weight_for_latency;
    public double weight_for_throughput;
    public double weight_for_cpu;
    public double min_cpu_threshold;
    public double max_cpu_threshold;
    public double max_throughput_threshold;
    public double min_throughput_threshold;
    public double max_cpu_threshold_for_localAdaptation;
    public double max_throughput_threshold_for_localAdaptation;

    public double thresholdForMaxAverageCpuUsageOfAllReplicas ;
    public double thresholdForMaxAverageThroughputOfAllReplicas ;
    public double thresholdForMinAverageThroughputOfAllReplicas ;
    public double thresholdForMinAverageCpuUsageOfAllReplicas ;

    @Override
    public String toString() {
        return "ApplicationSettings{" +
                "mood='" + mood + '\'' +
                ", metric_collection_interval_in_second=" + metric_collection_interval_in_second +
                ", max_acceptable_latency_in_second=" + max_acceptable_latency_in_second +
                ", weight_for_latency=" + weight_for_latency +
                ", weight_for_throughput=" + weight_for_throughput +
                ", weight_for_cpu=" + weight_for_cpu +
                ", min_cpu_threshold=" + min_cpu_threshold +
                ", max_cpu_threshold=" + max_cpu_threshold +
                ", max_throughput_threshold=" + max_throughput_threshold +
                ", min_throughput_threshold=" + min_throughput_threshold +
                ", max_cpu_threshold_for_localAdaptation=" + max_cpu_threshold_for_localAdaptation +
                ", max_throughput_threshold_for_localAdaptation=" + max_throughput_threshold_for_localAdaptation +
                ", thresholdForMaxAverageCpuUsageOfAllReplicas=" + thresholdForMaxAverageCpuUsageOfAllReplicas +
                ", thresholdForMaxAverageThroughputOfAllReplicas=" + thresholdForMaxAverageThroughputOfAllReplicas +
                ", thresholdForMinAverageThroughputOfAllReplicas=" + thresholdForMinAverageThroughputOfAllReplicas +
                ", thresholdForMinAverageCpuUsageOfAllReplicas=" + thresholdForMinAverageCpuUsageOfAllReplicas +
                '}';
    }

    public double getMax_cpu_threshold_for_localAdaptation() {
        return max_cpu_threshold_for_localAdaptation;
    }

    public void setMax_cpu_threshold_for_localAdaptation(double max_cpu_threshold_for_localAdaptation) {
        this.max_cpu_threshold_for_localAdaptation = max_cpu_threshold_for_localAdaptation;
    }

    public double getMax_throughput_threshold_for_localAdaptation() {
        return max_throughput_threshold_for_localAdaptation;
    }

    public void setMax_throughput_threshold_for_localAdaptation(double max_throughput_threshold_for_localAdaptation) {
        this.max_throughput_threshold_for_localAdaptation = max_throughput_threshold_for_localAdaptation;
    }

    public double getThresholdForMaxAverageCpuUsageOfAllReplicas() {
        return thresholdForMaxAverageCpuUsageOfAllReplicas;
    }

    public void setThresholdForMaxAverageCpuUsageOfAllReplicas(double thresholdForMaxAverageCpuUsageOfAllReplicas) {
        this.thresholdForMaxAverageCpuUsageOfAllReplicas = thresholdForMaxAverageCpuUsageOfAllReplicas;
    }

    public double getThresholdForMaxAverageThroughputOfAllReplicas() {
        return thresholdForMaxAverageThroughputOfAllReplicas;
    }

    public void setThresholdForMaxAverageThroughputOfAllReplicas(double thresholdForMaxAverageThroughputOfAllReplicas) {
        this.thresholdForMaxAverageThroughputOfAllReplicas = thresholdForMaxAverageThroughputOfAllReplicas;
    }

    public double getThresholdForMinAverageThroughputOfAllReplicas() {
        return thresholdForMinAverageThroughputOfAllReplicas;
    }

    public void setThresholdForMinAverageThroughputOfAllReplicas(double thresholdForMinAverageThroughputOfAllReplicas) {
        this.thresholdForMinAverageThroughputOfAllReplicas = thresholdForMinAverageThroughputOfAllReplicas;
    }

    public double getThresholdForMinAverageCpuUsageOfAllReplicas() {
        return thresholdForMinAverageCpuUsageOfAllReplicas;
    }

    public void setThresholdForMinAverageCpuUsageOfAllReplicas(double thresholdForMinAverageCpuUsageOfAllReplicas) {
        this.thresholdForMinAverageCpuUsageOfAllReplicas = thresholdForMinAverageCpuUsageOfAllReplicas;
    }

    public double getMin_throughput_threshold() {
        return min_throughput_threshold;
    }

    public void setMin_throughput_threshold(double min_throughput_threshold) {
        this.min_throughput_threshold = min_throughput_threshold;
    }

    public double getMax_throughput_threshold() {
        return max_throughput_threshold;
    }

    public void setMax_throughput_threshold(double max_throughput_threshold) {
        this.max_throughput_threshold = max_throughput_threshold;
    }


    public double getMin_cpu_threshold() {
        return min_cpu_threshold;
    }

    public void setMin_cpu_threshold(double min_cpu_threshold) {
        this.min_cpu_threshold = min_cpu_threshold;
    }

    public double getMax_cpu_threshold() {
        return max_cpu_threshold;
    }

    public void setMax_cpu_threshold(double max_cpu_threshold) {
        this.max_cpu_threshold = max_cpu_threshold;
    }

    public double getMax_acceptable_latency_in_second() {
        return max_acceptable_latency_in_second;
    }
    public void setMax_acceptable_latency_in_second(double max_acceptable_latency_in_second) {
        this.max_acceptable_latency_in_second = max_acceptable_latency_in_second;
    }
    public String getMood() {
        return mood;
    }

    public void setMood(String mood) {
        this.mood = mood;
    }

    public int getMetric_collection_interval_in_second() {
        return metric_collection_interval_in_second;
    }

    public void setMetric_collection_interval_in_second(int metric_collection_interval_in_second) {
        this.metric_collection_interval_in_second = metric_collection_interval_in_second;
    }


    public double getWeight_for_latency() {
        return weight_for_latency;
    }

    public void setWeight_for_latency(double weight_for_latency) {
        this.weight_for_latency = weight_for_latency;
    }

    public double getWeight_for_throughput() {
        return weight_for_throughput;
    }

    public void setWeight_for_throughput(double weight_for_throughput) {
        this.weight_for_throughput = weight_for_throughput;
    }

    public double getWeight_for_cpu() {
        return weight_for_cpu;
    }

    public void setWeight_for_cpu(double weight_for_cpu) {
        this.weight_for_cpu = weight_for_cpu;
    }


}
