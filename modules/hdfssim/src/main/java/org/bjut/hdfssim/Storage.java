/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.util.Id;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Storage implements Serializable {
    private int id;
    private double capacity;
    private double usedSize;
    private Map<Integer, Block> blockList; // blockId, Block
    private double maxTransferRate;
    private Datanode datanode;
    private int type;

    public static final int SSD = 0;
    public static final int HDD = 1;

    public Storage(Datanode datanode, double maxTransferRate, double capacity, int type) {
        this.id = Id.pollId(this.getClass());
        this.capacity = capacity;
        this.usedSize = 0;
        this.blockList = new HashMap<>();
        this.maxTransferRate = maxTransferRate;
        this.datanode = datanode;
        this.type = type;
    }

    public double getMaxTransferRate() {
        return maxTransferRate;
    }

    public void setMaxTransferRate(double maxTransferRate) {
        this.maxTransferRate = maxTransferRate;
    }

    public void addBlock(Block block) {
        this.blockList.put(block.getId(), block);
        block.setStorage(this);
        usedSize += block.getSize();
    }

    public void deleteBlock(Block block)
    {
        this.blockList.remove(block.getId());
        usedSize -= block.getSize();
        block.setStorage(null);
    }

    public boolean isFull(Block block) {
        if (this.usedSize + block.getSize() <= this.capacity) {
            return false;
        }
        return true;
    }

    public boolean isContainBlock(int id) {
        if (this.blockList.containsKey(id)) {
            return true;
        }
        return false;
    }

    public Datanode getDatanode() {
        return datanode;
    }

    public void setDatanode(Datanode datanode) {
        this.datanode = datanode;
    }

    public double getUsedSize() {
        return this.usedSize;
    }

    public boolean hasBlock(int blockId) {
        if (blockList.containsKey(blockId)) {
            return true;
        }
        return false;
    }

    public void reset()
    {
        this.usedSize = 0;
        this.blockList.clear();
    }

    public double getCapacity() {
        return capacity;
    }

    public Block getBlockById(int blockId)
    {
        return this.blockList.get(blockId);
    }

    public int getType() {
        return type;
    }
}
