package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.*;
import org.bjut.hdfssim.util.Id;

import java.io.Serializable;
import java.util.Random;

public class Datanode implements Serializable {
    private int id = Id.pollId(Datanode.class);
    private int rackId;
    private Storage hddStorage;
    private Storage ssdStorage;
    private HDFSHost host;

    public Datanode(int rackId, double hddCapacity, double hddMaxTransferRate, double ssdCapacity, double ssdMaxTransferRate, double bw, int coreNum, double mips) {
        this.rackId = rackId;
        this.hddStorage = new Storage(this, hddMaxTransferRate, hddCapacity);
        this.ssdStorage = new Storage(this, ssdMaxTransferRate, ssdCapacity);
        this.host = new HDFSHost(this, ssdMaxTransferRate, hddMaxTransferRate, bw, coreNum, mips);
    }


    public boolean addBlock(Block block) {
        if (isContainBlock(block.getId())) {
            return false;
        }
        Random random = new Random();
        if (random.nextInt(100) > 50) {
            if (!ssdStorage.isFull(block)) {
                ssdStorage.addBlock(block);
                return true;
            }
        }
        if (!hddStorage.isFull(block)) {
            hddStorage.addBlock(block);
            return true;
        }
        return false;
    }

    public boolean isFull(Block block) {
        if (hddStorage.isFull(block) && ssdStorage.isFull(block)) {
            return true;
        }
        return false;
    }

    public int getId() {
        return id;
    }

    public boolean isContainBlock(int id) {
        if (ssdStorage.isContainBlock(id) || hddStorage.isContainBlock(id)) {
            return true;
        }
        return false;
    }

    public int getRackId() {
        return rackId;
    }

    public double getSSDStorageUsage() {
        return ssdStorage.getUsedSize();
    }

    public double getHDDStorageUsage() {
        return hddStorage.getUsedSize();
    }

    public HDFSHost getHost() {
        return host;
    }
}
