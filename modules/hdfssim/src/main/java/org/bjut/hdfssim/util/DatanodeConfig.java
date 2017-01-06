package org.bjut.hdfssim.util;

import org.bjut.hdfssim.Storage;
import org.bjut.hdfssim.models.HDFS.Datanode;

public class DatanodeConfig {
    private int id;
    private int rackId;
    private double hddCapacity;
    private double hddMaxTransferRate;
    private double ssdCapacity;
    private double ssdMaxTransferRate;
    private double bw;
    private int coreNum;
    private double mips;

    public DatanodeConfig(Datanode datanode)
    {
        this.id = datanode.getId();
        this.rackId = datanode.getRackId();
        this.hddCapacity = datanode.getStorageByType(Storage.HDD).getCapacity();
        this.hddMaxTransferRate = datanode.getStorageByType(Storage.HDD).getMaxTransferRate();
        this.ssdCapacity = datanode.getStorageByType(Storage.SSD).getCapacity();
        this.ssdMaxTransferRate = datanode.getStorageByType(Storage.SSD).getMaxTransferRate();
        this.bw = datanode.getHost().getBw();
        this.coreNum = datanode.getHost().getCoreNum();
        this.mips = datanode.getHost().getMips();
    }

    public int getId() {
        return id;
    }

    public int getRackId() {
        return rackId;
    }

    public double getHddCapacity() {
        return hddCapacity;
    }

    public double getHddMaxTransferRate() {
        return hddMaxTransferRate;
    }

    public double getSsdCapacity() {
        return ssdCapacity;
    }

    public double getSsdMaxTransferRate() {
        return ssdMaxTransferRate;
    }

    public double getBw() {
        return bw;
    }

    public int getCoreNum() {
        return coreNum;
    }

    public double getMips() {
        return mips;
    }
}
