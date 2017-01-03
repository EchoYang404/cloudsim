package org.bjut.hdfssim.util;

public class DatanodeConfig {
    public int rackId;
    public int typeNum;
    public int count;

    public DatanodeConfig(int rackId, int typeNum, int count)
    {
        this.rackId = rackId;
        this.typeNum = typeNum;
        this.count = count;
    }
}
