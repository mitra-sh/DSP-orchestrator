package com.test.LoadBalancerDemo.streamProcessor.boltDetails;

public class Metrics {
    public double cpuLimit=0;
    public double cpu = 0;
    public double cpu_previous_cycle = 0;
    public float cpuAtBoltLevel = 0.0F;
    public float in_throughput = 0F;
    public float out_throughput = 0F;
    public float remaining_in_bandwidth_of_vm = 0F;
    public float remaining_out_bandwidth_of_vm = 0F;
    public float remaining_in_bandwidth_of_vm_in_percentage = 0F;
    public float remaining_out_bandwidth_of_vm_in_percentage = 0F;

    @Override
    public String toString() {
        return "Metrics{" +
                "cpuLimit=" + cpuLimit +
                ", cpu=" + cpu +
                ", cpu_previous_cycle=" + cpu_previous_cycle +
                ", cpuAtBoltLevel=" + cpuAtBoltLevel +
                ", in_throughput=" + in_throughput +
                ", out_throughput=" + out_throughput +
                ", remaining_in_bandwidth_of_vm=" + remaining_in_bandwidth_of_vm +
                ", remaining_out_bandwidth_of_vm=" + remaining_out_bandwidth_of_vm +
                ", remaining_in_bandwidth_of_vm_in_percentage=" + remaining_in_bandwidth_of_vm_in_percentage +
                ", remaining_out_bandwidth_of_vm_in_percentage=" + remaining_out_bandwidth_of_vm_in_percentage +
                '}';
    }

    public double getCpuLimit() {
        return cpuLimit;
    }

    public void setCpuLimit(double cpuLimit) {
        this.cpuLimit = cpuLimit;
    }

    public double getCpu_previous_cycle() {
        return cpu_previous_cycle;
    }

    public void setCpu_previous_cycle(double cpu_previous_cycle) {
        this.cpu_previous_cycle = cpu_previous_cycle;
    }

    public float getRemaining_in_bandwidth_of_vm_in_percentage() {
        return remaining_in_bandwidth_of_vm_in_percentage;
    }

    public void setRemaining_in_bandwidth_of_vm_in_percentage(float remaining_in_bandwidth_of_vm_in_percentage) {
        this.remaining_in_bandwidth_of_vm_in_percentage = remaining_in_bandwidth_of_vm_in_percentage;
    }

    public float getRemaining_out_bandwidth_of_vm_in_percentage() {
        return remaining_out_bandwidth_of_vm_in_percentage;
    }

    public void setRemaining_out_bandwidth_of_vm_in_percentage(float remaining_out_bandwidth_of_vm_in_percentage) {
        this.remaining_out_bandwidth_of_vm_in_percentage = remaining_out_bandwidth_of_vm_in_percentage;
    }


    public double getCpu() {
        return cpu;
    }

    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public float getCpuAtBoltLevel() {
        return cpuAtBoltLevel;
    }

    public void setCpuAtBoltLevel(float cpuAtBoltLevel) {
        this.cpuAtBoltLevel = cpuAtBoltLevel;
    }

    public float getIn_throughput() {
        return in_throughput;
    }

    public void setIn_throughput(float in_throughput) {
        this.in_throughput = in_throughput;
    }

    public float getOut_throughput() {
        return out_throughput;
    }

    public void setOut_throughput(float out_throughput) {
        this.out_throughput = out_throughput;
    }

    public float getRemaining_in_bandwidth_of_vm() {
        return remaining_in_bandwidth_of_vm;
    }

    public void setRemaining_in_bandwidth_of_vm(float remaining_in_bandwidth_of_vm) {
        this.remaining_in_bandwidth_of_vm = remaining_in_bandwidth_of_vm;
    }

    public float getRemaining_out_bandwidth_of_vm() {
        return remaining_out_bandwidth_of_vm;
    }

    public void setRemaining_out_bandwidth_of_vm(float remaining_out_bandwidth_of_vm) {
        this.remaining_out_bandwidth_of_vm = remaining_out_bandwidth_of_vm;
    }
}
