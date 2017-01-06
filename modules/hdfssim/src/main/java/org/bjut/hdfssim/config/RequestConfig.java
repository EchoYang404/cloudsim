package org.bjut.hdfssim.config;

public class RequestConfig {
    private double submitTime;
    private int rackId;
    private int datanodeId;
    private int hfileId;

    public RequestConfig(double submitTime, int rackId, int datanodeId, int hfileId) {
        this.submitTime = submitTime;
        this.rackId = rackId;
        this.datanodeId = datanodeId;
        this.hfileId = hfileId;
    }

    public double getSubmitTime() {
        return submitTime;
    }

    public int getRackId() {
        return rackId;
    }

    public int getDatanodeId() {
        return datanodeId;
    }

    public int getHfileId() {
        return hfileId;
    }
}
