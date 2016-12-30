package org.bjut.hdfssim;

import org.bjut.hdfssim.util.Id;

import java.util.List;

public class HDDStorage extends Storage {
    private int id;
    private double capacity;
    private List<Block> blockList;
    private double maxTransferRate;

    public HDDStorage(double capacity)
    {
        super(Id.pollId(HDDStorage.class),capacity);
        this.maxTransferRate = Configuration.getDoubleProperty("HDDMaxTransferRate");
    }

    public HDDStorage(int id, double capacity)
    {
        super(id,capacity);
        this.maxTransferRate = Configuration.getDoubleProperty("HDDMaxTransferRate");
    }
}
