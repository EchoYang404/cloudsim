package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.Storage;

import java.util.*;

public class DatanodeInfo {
    private Datanode datanode;
    private Map<Block, List<Double>> ssdHistory;
    private Map<Block, List<Double>> hddHistory;

    private int ssdAccessCount;
    private int hddAccessCount;

    public DatanodeInfo(Datanode datanode) {
        this.datanode = datanode;
        this.ssdHistory = new HashMap<>();
        this.hddHistory = new HashMap<>();
        this.ssdAccessCount = 0;
        this.hddAccessCount = 0;
    }

    public void addSsdBlock(Block block) {
        this.ssdHistory.put(block, new ArrayList<>());
    }

    public void addHddBlock(Block block) {
        this.hddHistory.put(block, new ArrayList<>());
    }

    public List<Double> deleteSsdBlock(Block block) {
        List<Double> value = this.ssdHistory.get(block);
        this.ssdHistory.remove(block);
        return value;
    }

    public List<Double> deleteHddBlock(Block block) {
        List<Double> value = this.hddHistory.get(block);
        this.hddHistory.remove(block);
        return value;
    }

    public void accessBlock(Block block, double time) {
        block.access();

        Map<Block, List<Double>> m;
        if (block.getStorage().getType() == Storage.SSD) {
            m = ssdHistory;
            ssdAccessCount++;
        } else {
            m = hddHistory;
            hddAccessCount++;
        }
        m.get(block).add(time);
    }

    public void finishAccessBlock(Block block) {
        block.finishAccess();
    }


    public void updateBlockHistory(Block block, List<Double> accessHistory) {
        if (ssdHistory.containsKey(block)) {
            ssdHistory.put(block, accessHistory);
        } else {
            hddHistory.put(block, accessHistory);
        }
    }

    public int getHddAccessCount() {
        return hddAccessCount;
    }

    public int getSsdAccessCount() {
        return ssdAccessCount;
    }

    public List<Block> getGreaterFromHdd(double frequecy, double time) {
        List<Block> result = new ArrayList<>();
        Iterator<Block> blockIterator = hddHistory.keySet().iterator();
        while (blockIterator.hasNext()) {
            Block block = blockIterator.next();
            if (hddHistory.get(block).size() == 0 || block.isMigrate()) {
                continue;
            }
            Iterator<Double> iterator = hddHistory.get(block).iterator();
            while (iterator.hasNext()) {
                double t = iterator.next();
                if (time - t > Configuration.getIntProperty("maxInterval")) {
                    iterator.remove();
                } else {
                    break;
                }
            }

            double f = (double) hddHistory.get(block).size() / Configuration.getIntProperty("maxInterval");
            if (f >= frequecy) {
                if (!block.isMigrate()) {
                    block.setMigrate(true);
                    result.add(block);
                }
            }
        }
        return result;
    }

    public List<Block> getLessFromSsd(double frequecy, double time) {
        List<Block> result = new ArrayList<>();
        // TODO 照着这个getGreaterFromHdd写
        return result;
    }
}
