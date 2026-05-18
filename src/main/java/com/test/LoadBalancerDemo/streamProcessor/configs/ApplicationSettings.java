package com.test.LoadBalancerDemo.streamProcessor.configs;

import java.io.Serializable;


import java.io.Serializable;

public class ApplicationSettings implements Serializable {
    public int maxNumberOfBoltsPerNode;
    public String initialEmitRate;
    public int initialDelayForSpoutToEmitDataInMilliSecond;

    public int metric_collection_interval_in_second;
    public int globalAdaptation_interval_in_second;

    public String mood;
    public int numberOfGenericBoltsInTheSystem;
    public int numberOfBoltsInTheSystem;
    public int numberOfSpoutsInTheSystem;
    public int numberOfSinksInTheSystem;

    public double weight_for_latency;
    public float weight_for_inBW_of_target;
    public float weight_for_outBW_of_target;
    public double weight_for_cpu;

    public double min_cpu_threshold;
    public double max_cpu_threshold;
    public float min_remaining_bandwidth_threshold;
    public double max_throughput_threshold;
    public double min_throughput_threshold;
    public double max_cpu_threshold_for_localAdaptation;
    public double max_throughput_threshold_for_localAdaptation;
    public float min_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation;
    public double thresholdForMaxAverageCpuUsageOfAllReplicas;
    public double replicasMinAvgRemainingBandwidthThreshold;
    public float min_RBW;
    public float adjustmentCoefficient;
    public float freeThresholdForCPU;
    public float freeThresholdForRemainingBW;
    public int totalNumberOfOpsInStormBaselines;
    public double responsivenessCoEfficientToBN;

    @Override
    public String toString() {
        return "ApplicationSettings{" +
                "maxNumberOfBoltsPerNode=" + maxNumberOfBoltsPerNode +
                ", initialEmitRate='" + initialEmitRate + '\'' +
                ", initialDelayForSpoutToEmitDataInMilliSecond=" + initialDelayForSpoutToEmitDataInMilliSecond +
                ", metric_collection_interval_in_second=" + metric_collection_interval_in_second +
                ", globalAdaptation_interval_in_second=" + globalAdaptation_interval_in_second +
                ", mood='" + mood + '\'' +
                ", numberOfGenericBoltsInTheSystem=" + numberOfGenericBoltsInTheSystem +
                ", numberOfBoltsInTheSystem=" + numberOfBoltsInTheSystem +
                ", numberOfSpoutsInTheSystem=" + numberOfSpoutsInTheSystem +
                ", numberOfSinksInTheSystem=" + numberOfSinksInTheSystem +
                ", weight_for_latency=" + weight_for_latency +
                ", weight_for_inBW_of_target=" + weight_for_inBW_of_target +
                ", weight_for_outBW_of_target=" + weight_for_outBW_of_target +
                ", weight_for_cpu=" + weight_for_cpu +
                ", min_cpu_threshold=" + min_cpu_threshold +
                ", max_cpu_threshold=" + max_cpu_threshold +
                ", min_remaining_bandwidth_threshold=" + min_remaining_bandwidth_threshold +
                ", max_throughput_threshold=" + max_throughput_threshold +
                ", min_throughput_threshold=" + min_throughput_threshold +
                ", max_cpu_threshold_for_localAdaptation=" + max_cpu_threshold_for_localAdaptation +
                ", max_throughput_threshold_for_localAdaptation=" + max_throughput_threshold_for_localAdaptation +
                ", min_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation=" + min_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation +
                ", thresholdForMaxAverageCpuUsageOfAllReplicas=" + thresholdForMaxAverageCpuUsageOfAllReplicas +
                ", replicasMinAvgRemainingBandwidthThreshold=" + replicasMinAvgRemainingBandwidthThreshold +
                ", min_RBW=" + min_RBW +
                ", adjustmentCoefficient=" + adjustmentCoefficient +
                ", freeThresholdForCPU=" + freeThresholdForCPU +
                ", freeThresholdForRemainingBW=" + freeThresholdForRemainingBW +
                ", totalNumberOfOpsInStormBaselines=" + totalNumberOfOpsInStormBaselines +
                ", responsivenessCoEfficientToBN=" + responsivenessCoEfficientToBN +
                '}';
    }

    public int getMaxNumberOfBoltsPerNode() {
        return maxNumberOfBoltsPerNode;
    }

    public double getResponsivenessCoEfficientToBN() {
        return responsivenessCoEfficientToBN;
    }

    public void setResponsivenessCoEfficientToBN(double responsivenessCoEfficientToBN) {
        this.responsivenessCoEfficientToBN = responsivenessCoEfficientToBN;
    }

    public int gatMaxNumberOfBoltsPerNode() {
        return maxNumberOfBoltsPerNode;
    }

    public void setMaxNumberOfBoltsPerNode(int maxNumberOfBoltsPerNode) {
        this.maxNumberOfBoltsPerNode = maxNumberOfBoltsPerNode;
    }

    public int getTotalNumberOfOpsInStormBaselines() {
        return totalNumberOfOpsInStormBaselines;
    }

    public void setTotalNumberOfOpsInStormBaselines(int totalNumberOfOpsInStormBaselines) {
        this.totalNumberOfOpsInStormBaselines = totalNumberOfOpsInStormBaselines;
    }

    public float getFreeThresholdForCPU() {
        return freeThresholdForCPU;
    }

    public void setFreeThresholdForCPU(float freeThresholdForCPU) {
        this.freeThresholdForCPU = freeThresholdForCPU;
    }

    public float getFreeThresholdForRemainingBW() {
        return freeThresholdForRemainingBW;
    }

    public void setFreeThresholdForRemainingBW(float freeThresholdForRemainingBW) {
        this.freeThresholdForRemainingBW = freeThresholdForRemainingBW;
    }


    public float getAdjustmentCoefficient() {
        return adjustmentCoefficient;
    }

    public void setAdjustmentCoefficient(float adjustmentCoefficient) {
        this.adjustmentCoefficient = adjustmentCoefficient;
    }


    public float getWeight_for_outBW_of_target() {
        return weight_for_outBW_of_target;
    }

    public void setWeight_for_outBW_of_target(float weight_for_outBW_of_target) {
        this.weight_for_outBW_of_target = weight_for_outBW_of_target;
    }

    public int getGlobalAdaptation_interval_in_second() {
        return globalAdaptation_interval_in_second;
    }

    public void setGlobalAdaptation_interval_in_second(int globalAdaptation_interval_in_second) {
        this.globalAdaptation_interval_in_second = globalAdaptation_interval_in_second;
    }

    public int getInitialDelayForSpoutToEmitDataInMilliSecond() {
        return initialDelayForSpoutToEmitDataInMilliSecond;
    }

    public void setInitialDelayForSpoutToEmitDataInMilliSecond(int initialDelayForSpoutToEmitDataInMilliSecond) {
        this.initialDelayForSpoutToEmitDataInMilliSecond = initialDelayForSpoutToEmitDataInMilliSecond;
    }

    public String getInitialEmitRate() {
        return initialEmitRate;
    }

    public void setInitialEmitRate(String initialEmitRate) {
        this.initialEmitRate = initialEmitRate;
    }

    public int getNumberOfGenericBoltsInTheSystem() {
        return numberOfGenericBoltsInTheSystem;
    }

    public void setNumberOfGenericBoltsInTheSystem(int numberOfGenericBoltsInTheSystem) {
        this.numberOfGenericBoltsInTheSystem = numberOfGenericBoltsInTheSystem;
    }

    public int getNumberOfBoltsInTheSystem() {
        return numberOfBoltsInTheSystem;
    }

    public void setNumberOfBoltsInTheSystem(int numberOfBoltsInTheSystem) {
        this.numberOfBoltsInTheSystem = numberOfBoltsInTheSystem;
    }

    public int getNumberOfSpoutsInTheSystem() {
        return numberOfSpoutsInTheSystem;
    }

    public void setNumberOfSpoutsInTheSystem(int numberOfSpoutsInTheSystem) {
        this.numberOfSpoutsInTheSystem = numberOfSpoutsInTheSystem;
    }

    public int getNumberOfSinksInTheSystem() {
        return numberOfSinksInTheSystem;
    }

    public void setNumberOfSinksInTheSystem(int numberOfSinksInTheSystem) {
        this.numberOfSinksInTheSystem = numberOfSinksInTheSystem;
    }

    public float getMin_RBW() {
        return min_RBW;
    }

    public void setMin_RBW(float min_RBW) {
        this.min_RBW = min_RBW;
    }

    public float getMin_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation() {
        return min_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation;
    }

    public void setMin_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation(float min_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation) {
        this.min_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation = min_remaining_in_bandwidth_threshold_in_percentage_for_localAdaptation;
    }



    public float getMin_remaining_bandwidth_threshold() {
        return min_remaining_bandwidth_threshold;
    }

    public void setMin_remaining_bandwidth_threshold(float min_remaining_bandwidth_threshold) {
        this.min_remaining_bandwidth_threshold = min_remaining_bandwidth_threshold;
    }

    public float getWeight_for_inBW_of_target() {
        return weight_for_inBW_of_target;
    }

    public void setWeight_for_inBW_of_target(float weight_for_inBW_of_target) {
        this.weight_for_inBW_of_target = weight_for_inBW_of_target;
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

    public double getReplicasMinAvgRemainingBandwidthThreshold() {
        return replicasMinAvgRemainingBandwidthThreshold;
    }

    public void setReplicasMinAvgRemainingBandwidthThreshold(double replicasMinAvgRemainingBandwidthThreshold) {
        this.replicasMinAvgRemainingBandwidthThreshold = replicasMinAvgRemainingBandwidthThreshold;
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


    public double getWeight_for_cpu() {
        return weight_for_cpu;
    }

    public void setWeight_for_cpu(double weight_for_cpu) {
        this.weight_for_cpu = weight_for_cpu;
    }


}
