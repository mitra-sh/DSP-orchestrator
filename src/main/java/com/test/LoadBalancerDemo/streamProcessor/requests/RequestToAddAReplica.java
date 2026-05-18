package com.test.LoadBalancerDemo.streamProcessor.requests;

public class RequestToAddAReplica {
    public String nameOfClassLoadedInside;
    public String newReplica;
    public String originalBoltName;


    public RequestToAddAReplica(String nameOfClassLoadedInside, String newReplica, String originalBoltName) {
        this.nameOfClassLoadedInside = nameOfClassLoadedInside;
        this.newReplica = newReplica;
        this.originalBoltName = originalBoltName;
    }

    @Override
    public String toString() {
        return "RequestToAddAReplica{" +
                "nameOfClassLoadedInside='" + nameOfClassLoadedInside + '\'' +
                ", newReplica='" + newReplica + '\'' +
                ", originalBoltName='" + originalBoltName + '\'' +
                '}';
    }

    public String getNameOfClassLoadedInside() {
        return nameOfClassLoadedInside;
    }

    public void setNameOfClassLoadedInside(String nameOfClassLoadedInside) {
        this.nameOfClassLoadedInside = nameOfClassLoadedInside;
    }

    public String getNewReplica() {
        return newReplica;
    }

    public void setNewReplica(String newReplica) {
        this.newReplica = newReplica;
    }

    public String getOriginalBoltName() {
        return originalBoltName;
    }

    public void setOriginalBoltName(String originalBoltName) {
        this.originalBoltName = originalBoltName;
    }
}
