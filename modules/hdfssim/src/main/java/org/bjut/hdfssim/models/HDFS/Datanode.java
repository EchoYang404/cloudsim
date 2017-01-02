package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HDDStorage;
import org.bjut.hdfssim.SSDStorage;
import org.bjut.hdfssim.Storage;
import org.bjut.hdfssim.util.Id;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.List;
import java.util.Random;

public class Datanode implements Serializable {
    private int id = Id.pollId(Datanode.class);
    private int rackId;
    private HDDStorage hddStorage;
    private SSDStorage ssdStorage;
    //TODO
    transient private Host host;

    public Datanode(int rackId) {
        this.rackId = rackId;
    }

    public Datanode(int rackId, int hddCapacity, int ssdCapacity)
    {
        this.rackId = rackId;
        this.hddStorage = new HDDStorage(hddCapacity,this);
        this.ssdStorage = new SSDStorage(ssdCapacity,this);
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

    public double getSSDStorageUsage()
    {
        return ssdStorage.getUsedSize();
    }

    public double getHDDStorageUsage()
    {
        return hddStorage.getUsedSize();
    }
}
