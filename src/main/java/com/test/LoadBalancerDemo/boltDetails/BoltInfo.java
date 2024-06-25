package com.test.LoadBalancerDemo.boltDetails;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

public class BoltInfo {
    public String componentName;
    public String nameOfClassLoadedInside;
    public String hostName = "none";
    public double cpu = 0;
    public double throughput = 0;
    public int taskId = 0;
    //it varies for different upstream bolts
    public int serverPort = 0;


    @Override
    public String toString() {
        return "BoltInfo{" +
                "componentName='" + componentName + '\'' +
                ", nameOfClassLoadedInside='" + nameOfClassLoadedInside + '\'' +
                ", hostName='" + hostName + '\'' +
                ", cpu=" + cpu +
                ", throughput=" + throughput +
                ", taskId=" + taskId +
                ", serverPort=" + serverPort +
                '}';
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public String getNameOfClassLoadedInside() {
        return nameOfClassLoadedInside;
    }

    public void setNameOfClassLoadedInside(String nameOfClassLoadedInside) {
        this.nameOfClassLoadedInside = nameOfClassLoadedInside;
    }

    public double getCpu() {
        return cpu;
    }

    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public double getThroughput() {
        return throughput;
    }

    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }


}
