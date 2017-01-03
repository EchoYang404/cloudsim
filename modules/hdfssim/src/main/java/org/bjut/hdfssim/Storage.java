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
    private int id = Id.pollId(this.getClass());
    private double capacity;
    private double usedSize;
    private Map<Integer, Block> blockList;
    private double maxTransferRate;
    private Datanode datanode;

    public Storage(Datanode datanode, double maxTransferRate, double capacity) {
        this.capacity = capacity;
        this.usedSize = 0;
        this.blockList = new HashMap<>();
        this.maxTransferRate = maxTransferRate;
        this.datanode = datanode;
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
}
