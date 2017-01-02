package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.util.Id;

import java.util.List;

public class SSDStorage extends Storage {
    public SSDStorage(double capacity)
    {
        super(Id.pollId(SSDStorage.class),capacity);
        this.setMaxTransferRate(Configuration.getDoubleProperty("SSDMaxTransferRate"));
    }

    public SSDStorage(double capacity, Datanode datanode)
    {
        super(Id.pollId(SSDStorage.class),capacity,datanode);
        this.setMaxTransferRate(Configuration.getDoubleProperty("SSDMaxTransferRate"));
    }
}
