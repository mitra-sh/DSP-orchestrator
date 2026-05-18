package com.test.LoadBalancerDemo.streamProcessor.boltDetails;

public class BoltInfo {
    public String componentName;
    public String nameOfClassLoadedInside;
    public String supervisorNameForTCPConnection = "none";
    public String hostName = "none";
    public Metrics metrics;
    public int taskId = 0;
    //it varies for different upstream bolts
    public int serverPort = 0;
    public BoltInfo() {
        this.metrics = new Metrics();
    }
    @Override
    public String toString() {
        return "BoltInfo{" +
                "componentName='" + componentName + '\'' +
                ", nameOfClassLoadedInside='" + nameOfClassLoadedInside + '\'' +
                ", supervisorNameForTCPConnection='" + supervisorNameForTCPConnection + '\'' +
                ", hostName='" + hostName + '\'' +
                ", taskId=" + taskId +
                ", serverPort=" + serverPort +
                ", metrics=" + metrics +
                '}';
    }


    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    public String getSupervisorNameForTCPConnection() {
        return supervisorNameForTCPConnection;
    }

    public void setSupervisorNameForTCPConnection(String supervisorNameForTCPConnection) {
        this.supervisorNameForTCPConnection = supervisorNameForTCPConnection;
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


    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

}
