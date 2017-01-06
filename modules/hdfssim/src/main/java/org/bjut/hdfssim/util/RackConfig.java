package org.bjut.hdfssim.util;

public class RackConfig {
    private int rackId;
    private int typeNum;
    private int count;

    public RackConfig(int rackId, int typeNum, int count)
    {
        this.rackId = rackId;
        this.typeNum = typeNum;
        this.count = count;
    }

    public int getRackId() {
        return rackId;
    }

    public int getTypeNum() {
        return typeNum;
    }

    public int getCount() {
        return count;
    }
}
