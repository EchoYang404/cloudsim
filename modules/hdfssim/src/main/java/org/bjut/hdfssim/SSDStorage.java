package org.bjut.hdfssim;

import org.bjut.hdfssim.util.Id;

import java.util.List;

public class SSDStorage extends Storage {

    private int id;
    private double capacity;
    private List<Block> blockList;
    private double maxTransferRate;

    public SSDStorage(double capacity)
    {
        super(Id.pollId(SSDStorage.class),capacity);
        maxTransferRate = Configuration.getDoubleProperty("SSDMaxTransferRate");
    }

    public SSDStorage(int id, double capacity)
    {
        super(id,capacity);
        maxTransferRate = Configuration.getDoubleProperty("SSDMaxTransferRate");
    }
}
