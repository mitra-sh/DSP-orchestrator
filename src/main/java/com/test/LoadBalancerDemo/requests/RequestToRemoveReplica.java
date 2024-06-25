package com.test.LoadBalancerDemo.requests;

public class RequestToRemoveReplica {
    public String originalBoltName;
    public String boltNameToDelete;

    public RequestToRemoveReplica(String originalBoltName, String boltNameToDelete) {
        this.originalBoltName = originalBoltName;
        this.boltNameToDelete = boltNameToDelete;
    }

    public String getOriginalBoltName() {
        return originalBoltName;
    }

    public void setOriginalBoltName(String originalBoltName) {
        this.originalBoltName = originalBoltName;
    }

    public String getBoltNameToDelete() {
        return boltNameToDelete;
    }

    public void setBoltNameToDelete(String boltNameToDelete) {
        this.boltNameToDelete = boltNameToDelete;
    }
}
