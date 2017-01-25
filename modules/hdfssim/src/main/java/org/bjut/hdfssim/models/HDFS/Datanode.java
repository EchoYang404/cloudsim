package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.*;
import org.bjut.hdfssim.config.DatanodeConfig;
import org.bjut.hdfssim.util.Id;
import org.cloudbus.cloudsim.Log;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

public class Datanode implements Serializable {
    private int id;
    private int rackId;
    private Storage hddStorage;
    private Storage ssdStorage;
    private DatanodeInfo info;
    private HDFSHost host;

    public Datanode() {
    }

    public Datanode(int rackId, double hddCapacity, double hddMaxTransferRate, double ssdCapacity, double
            ssdMaxTransferRate, double bw, int coreNum, double mips) {
        this.id = Id.pollId(this.getClass());
        this.rackId = rackId;
        this.hddStorage = new Storage(this, hddMaxTransferRate, hddCapacity, Storage.SSD);
        this.ssdStorage = new Storage(this, ssdMaxTransferRate, ssdCapacity, Storage.HDD);
        this.host = new HDFSHost(this, ssdMaxTransferRate, hddMaxTransferRate, bw, coreNum, mips);
        this.info = new DatanodeInfo(this);
    }

    public Datanode(DatanodeConfig config) {
        this.id = config.getId();
        this.rackId = config.getRackId();
        this.hddStorage = new Storage(this, config.getHddMaxTransferRate(), config.getHddCapacity(), Storage.HDD);
        this.ssdStorage = new Storage(this, config.getSsdMaxTransferRate(), config.getSsdCapacity(), Storage.SSD);
        this.host = new HDFSHost(this, config.getSsdMaxTransferRate(), config.getHddMaxTransferRate(), config.getBw()
                , config.getCoreNum(), config.getMips());
        this.info = new DatanodeInfo(this);
    }

    public int addBlockToStorage(Block block, int type) {
        if (type == Storage.HDD) {
            if (!hddStorage.isFull(block)) {
                hddStorage.addBlock(block);
                info.addHddBlock(block);
                return Storage.HDD;
            }
        }

        if (type == Storage.SSD) {
            if (!ssdStorage.isFull(block)) {
                ssdStorage.addBlock(block);
                info.addSsdBlock(block);
                return Storage.SSD;
            }
        }
        // 返回-1表示为添加失败
        return -1;
    }

    public int addBlockToStorage(Block block, List<Double> accessHistory, int type) {
        int value = addBlockToStorage(block, type);
        if (value == -1) return value;
        this.info.updateBlockHistory(block, accessHistory);
        return value;
    }

    public List<Double> deleteBlock(Block block) {
        int type = block.getStorage().getType();
        if (type == Storage.SSD) {
            return this.info.deleteSsdBlock(block);
        } else {
            return this.info.deleteHddBlock(block);
        }
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
        if (ssdStorage.isContainBlock(blockId)) return Storage.SSD;
        if (hddStorage.isContainBlock(blockId)) return Storage.HDD;
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

    public double getSSDUtilization() {
        return ssdStorage.getUsedSize() / ssdStorage.getCapacity();
    }

    public Block getBlockById(int blockId) {
        Block block = null;
        if (ssdStorage.isContainBlock(blockId)) {
            block = ssdStorage.getBlockById(blockId);
            if(ssdStorage.getBlockById(blockId).isMigrate() == false){
                return ssdStorage.getBlockById(blockId);
            }
        }

        if (hddStorage.isContainBlock(blockId)) {
            block = hddStorage.getBlockById(blockId);
            if(hddStorage.getBlockById(blockId).isMigrate() == false){
                return hddStorage.getBlockById(blockId);
            }
        }
        try {
            throw new Exception(block.isMigrate() + " there is no block " + blockId + " in  datanode "+ this.getId() +"!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void accessBlock(Block block, double time) {
        this.info.accessBlock(block, time);
    }

    public void finishAccessBlock(Block block) {
        this.info.finishAccessBlock(block);
    }

    public DatanodeInfo getInfo() {
        return info;
    }

    public Storage getSsdStorage() {
        return ssdStorage;
    }

    public Storage getHddStorage() {
        return hddStorage;
    }
}
