package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.util.Id;

import java.util.List;

public class HDDStorage extends Storage {

    public HDDStorage(double capacity) {
        super(Id.pollId(HDDStorage.class), capacity);
        this.setMaxTransferRate(Configuration.getDoubleProperty("HDDMaxTransferRate"));
    }

    public HDDStorage(double capacity, Datanode datanode) {
        super(Id.pollId(HDDStorage.class), capacity, datanode);
        this.setMaxTransferRate(Configuration.getDoubleProperty("HDDMaxTransferRate"));
    }
}
