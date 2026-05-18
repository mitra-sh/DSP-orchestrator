package com.test.LoadBalancerDemo.streamProcessor.boltDetails;

public class BottleneckDetails {
    public String vmName;
    public String cause;
    public double usagePercentage;

    public BottleneckDetails(String vmName,String cause, double usagePercentage) {
        this.vmName = vmName;
        this.cause = cause;
        this.usagePercentage = usagePercentage;
    }

    public BottleneckDetails() {

    }

    public String getVmName() {
        return vmName;
    }

    public void setVmName(String vmName) {
        this.vmName = vmName;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public double getUsagePercentage() {
        return usagePercentage;
    }

    public void setUsagePercentage(double usagePercentage) {
        this.usagePercentage = usagePercentage;
    }

    @Override
    public String toString() {
        return "BottleneckDetails{" +
                "vmName='" + vmName + '\'' +
                ", cause='" + cause + '\'' +
                ", usagePercentage=" + usagePercentage +
                '}';
    }
}
