package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.*;
import org.bjut.hdfssim.util.DatanodeConfig;
import org.bjut.hdfssim.util.Id;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.Random;

public class Datanode implements Serializable {
    private int id;
    private int rackId;
    private Storage hddStorage;
    private Storage ssdStorage;
    private HDFSHost host;

    public Datanode() {
    }

    public Datanode(int rackId, double hddCapacity, double hddMaxTransferRate, double ssdCapacity, double
            ssdMaxTransferRate, double bw, int coreNum, double mips) {
        this.id = Id.pollId(Datanode.class);
        this.rackId = rackId;
        this.hddStorage = new Storage(this, hddMaxTransferRate, hddCapacity);
        this.ssdStorage = new Storage(this, ssdMaxTransferRate, ssdCapacity);
        this.host = new HDFSHost(this, ssdMaxTransferRate, hddMaxTransferRate, bw, coreNum, mips);
    }

    public Datanode(DatanodeConfig config) {
        this.id = config.getId();
        this.rackId = config.getRackId();
        this.hddStorage = new Storage(this, config.getHddMaxTransferRate(), config.getHddCapacity());
        this.ssdStorage = new Storage(this, config.getSsdMaxTransferRate(), config.getSsdCapacity());
        this.host = new HDFSHost(this, config.getSsdMaxTransferRate(), config.getHddMaxTransferRate(), config.getBw()
                , config.getCoreNum(), config.getMips());
    }


    public int addBlock(Block block) {
        if (isContainBlock(block.getId())) {
            return -1;
        }
        Random random = new Random();
        if (random.nextInt(100) > 50) {
            if (!ssdStorage.isFull(block)) {
                ssdStorage.addBlock(block);
                return Storage.SSD;
            }
        }
        if (!hddStorage.isFull(block)) {
            hddStorage.addBlock(block);
            return Storage.HDD;
        }
        return -1;
    }

    public int addBlockToStorage(Block block, int type) {

        if (isContainBlock(block.getId())) {
            return -1;
        }
        if(type == Storage.HDD)
        {
            if (!hddStorage.isFull(block)) {
                hddStorage.addBlock(block);
                return Storage.HDD;
            }
        }

        if(type == Storage.SSD)
        {
            if (!ssdStorage.isFull(block)) {
                ssdStorage.addBlock(block);
                return Storage.SSD;
            }
        }
        // 返回-1表示为添加失败
        return -1;
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

    public HDFSHost getHost() {
        return host;
    }

    public int getDistance(Datanode datanode) {
        if (this.getId() == this.getId()) return 0;
        if (this.getRackId() == datanode.getRackId()) return 2;
        return 4;
    }

    public int getStorageTypeByBlockId(int blockId) {
        if (ssdStorage.hasBlock(blockId)) return Storage.SSD;
        if (hddStorage.hasBlock(blockId)) return Storage.HDD;
        return -1;
    }

    public void resetStorages() {
        this.ssdStorage.reset();
        this.hddStorage.reset();
    }

    public Storage getStorageByType(int type) {
        if (type == Storage.HDD) {
            return hddStorage;
        }
        if (type == Storage.SSD) {
            return ssdStorage;
        }
        try {
            throw new Exception("There is no datanode has type = " + type + "!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
